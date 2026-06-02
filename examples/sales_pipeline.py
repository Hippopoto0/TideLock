"""
examples/sales_pipeline.py  —  CSV data processing with checkpointing
=====================================================================

A pipeline that generates synthetic sales data, loads it as a DataFrame,
cleans and transforms it, computes summary statistics, and exports results.
DataFrames are checkpointed as CSV between steps, making intermediate
state human-readable in the TUI inspector.

Steps:
  generate_data  — write a CSV of synthetic sales records
  load_data      — read CSV into a DataFrame on shared state
  clean_data     — filter invalid rows and add derived columns
  compute_summary — groupby aggregation
  export_results — write summary & cleaned data to disk

Run:
    cd /Users/dan/pelagic/TideLock
    uv run python examples/sales_pipeline.py run
    uv run python examples/sales_pipeline.py inspect
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import typer

from tidelock.engine import Flow, pipeline, step
from tidelock.engine import PipelineState as TLState

_LOGS_DIR = Path(__file__).parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)

_app = typer.Typer(no_args_is_help=True)

CATEGORIES = ["Electronics", "Clothing", "Home", "Books", "Sports"]
STATUSES = ["shipped", "pending", "cancelled", "returned"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]


class PipelineState(TLState):
    model_config = {"arbitrary_types_allowed": True}
    csv_path: str = ""
    orders: pd.DataFrame = pd.DataFrame()
    cleaned: pd.DataFrame = pd.DataFrame()
    summary: pd.DataFrame = pd.DataFrame()


@step("generate_data")
def generate_data(shared: PipelineState) -> None:
    path = _LOGS_DIR / "sales_orders.csv"
    n = 200
    rows: list[dict] = []
    base = datetime(2025, 1, 1)
    for i in range(n):
        qty = random.randint(1, 20)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        rows.append(
            {
                "order_id": f"ORD-{1000 + i}",
                "date": (base + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                "category": random.choice(CATEGORIES),
                "product": f"Product-{random.randint(1, 50)}",
                "quantity": qty,
                "unit_price": unit_price,
                "total": round(qty * unit_price, 2),
                "currency": random.choice(CURRENCIES),
                "status": random.choice(STATUSES),
                "customer_id": f"CUST-{random.randint(100, 999)}",
            }
        )
    rows[0]["quantity"] = -5
    rows[1]["total"] = None
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    shared.csv_path = str(path)
    typer.echo(f"    generated {n} orders → {path}")


@step("load_data")
def load_data(shared: PipelineState) -> None:
    df = pd.read_csv(shared.csv_path)
    shared.orders = df
    typer.echo(f"    loaded {len(df)} orders")


@step("clean_data")
def clean_data(shared: PipelineState) -> None:
    df = shared.orders.copy()
    before = len(df)
    df = df[df["quantity"] > 0]
    df = df.dropna(subset=["total"])
    df = df[df["total"] > 0]
    rates = {"USD": 1.0, "EUR": 1.08, "GBP": 1.26, "JPY": 0.0067}
    df["revenue_usd"] = df.apply(
        lambda r: r["total"] * rates.get(r["currency"], 1.0),
        axis=1,
    )
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M").astype(str)
    shared.cleaned = df.reset_index(drop=True)
    typer.echo(f"    cleaned: {before} → {len(df)} rows")


@step("compute_summary")
def compute_summary(shared: PipelineState) -> None:
    summary = (
        shared.cleaned.groupby(["category", "month"], as_index=False)
        .agg(total_orders=("order_id", "count"), total_revenue=("revenue_usd", "sum"))
        .round(2)
        .sort_values(["category", "month"])
    )
    shared.summary = summary
    typer.echo(f"    summary: {len(summary)} rows")


@step("export_results")
def export_results(shared: PipelineState) -> None:
    shared.cleaned.to_csv(_LOGS_DIR / "cleaned_orders.csv", index=False)
    shared.summary.to_csv(_LOGS_DIR / "summary_by_category.csv", index=False)
    typer.echo(f"    cleaned → {_LOGS_DIR / 'cleaned_orders.csv'}")
    typer.echo(f"    summary → {_LOGS_DIR / 'summary_by_category.csv'}")


@pipeline()
def build() -> Flow:
    return Flow(
        generate_data >> load_data,
        load_data >> clean_data,
        clean_data >> compute_summary,
        compute_summary >> export_results,
        state_cls=PipelineState,
        on_node_start=lambda n: typer.echo(f"▸ {n}"),
        on_node_skip=lambda n: typer.echo(f"↩ skip {n}"),
    )


@_app.command()
def run() -> None:
    """Execute the pipeline from the start node."""
    from tidelock.engine import start_flow

    start_flow("run")


@_app.command()
def resume(
    pid: str | None = typer.Argument(None, help="Run PID to resume from (defaults to most recent)"),
    from_node: str | None = typer.Option(None, "--from-node", "-f"),
) -> None:
    """Resume a previous run from a chosen node."""
    from tidelock.engine import _most_recent_run, branch_run, prompt_select_node, start_flow

    if pid is None:
        pid = _most_recent_run()
        if pid is None:
            typer.secho("No runs found.", fg=typer.colors.RED)
            raise typer.Exit(1)
        typer.echo(f"Using most recent run: {pid}")
    if from_node is None:
        from_node = prompt_select_node(pid, "Select node to resume from:", checkpointed_only=False)
        if not from_node:
            return
    new_pid = branch_run(pid, from_node)
    start_flow("resume", new_pid, from_node)


@_app.command()
def inspect(
    pid: str | None = typer.Argument(None, help="Run PID (defaults to most recent)"),
) -> None:
    """Open the TUI inspector."""
    from tidelock.engine import _most_recent_run, prompt_select_node
    from tidelock.tui import NodeInspectorApp

    if pid is None:
        pid = _most_recent_run()
        if pid is None:
            typer.secho("No runs found.", fg=typer.colors.RED)
            raise typer.Exit(1)
        typer.echo(f"Using most recent run: {pid}")
    node = prompt_select_node(pid, "Select node to inspect:")
    if node:
        NodeInspectorApp(pid, node).run()


if __name__ == "__main__":
    _app()
