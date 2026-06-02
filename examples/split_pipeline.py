"""
examples/split_pipeline.py  —  Concurrent steps with split()
=============================================================

A content-aggregation pipeline that fetches from three independent
sources in parallel, then merges and summarises the results.

  plan      — decide what to fetch
  split(    — all three run concurrently
    fetch_news,
    fetch_filings,
    fetch_reports,
  )
  merge     — combine into a single findings list
  summarise — write a short brief to disk

Run:
    cd /Users/dan/pelagic/TideLock
    uv run python examples/split_pipeline.py run
    uv run python examples/split_pipeline.py inspect

The three fetch steps each simulate ~0.3 s of I/O. Running them
sequentially would take ~0.9 s; split() runs them concurrently so
the wall time stays around ~0.3 s.

Resume demo — to see partial-failure recovery:
    uv run python examples/split_pipeline.py run --fail-filings
    uv run python examples/split_pipeline.py resume <pid>
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer

from tidelock.engine import Flow, RetryPolicy, pipeline, split, step
from tidelock.engine import PipelineState as TLState

_LOGS_DIR = Path(__file__).parent / "logs"
_LOGS_DIR.mkdir(exist_ok=True)

# Allow an optional --fail-filings flag to demonstrate partial failure + resume
_app = typer.Typer(no_args_is_help=True)
_fail_filings: bool = False


# ── State ─────────────────────────────────────────────────────────────────────


class PipelineState(TLState):
    topic: str = "renewable energy investment landscape"
    queries: list[str]
    news: list[str]
    filings: list[str]
    reports: list[str]
    findings: list[str]
    brief: str = ""


# ── Simulated data ────────────────────────────────────────────────────────────

_NEWS = [
    "Solar capacity additions hit record 350 GW globally in 2024.",
    "Offshore wind financing rounds topped $40 bn in Q3.",
    "Battery storage costs fell 18 % year-on-year.",
]

_FILINGS = [
    "Form 10-K: NextEra Energy reports 12 % revenue growth.",
    "S-1 filing: GridEdge raises $180 m Series C for grid software.",
    "8-K: Vestas wins 2.1 GW turbine order across three continents.",
]

_REPORTS = [
    "BloombergNEF: renewable capex to exceed fossil fuels by 2026.",
    "IEA: 90 % of new power capacity in 2024 came from renewables.",
    "Wood Mackenzie: grid-scale storage pipeline triples in 18 months.",
]


# ── Steps ─────────────────────────────────────────────────────────────────────


@step("plan")
async def plan(shared: PipelineState) -> None:
    await asyncio.sleep(0.05)
    shared.queries = [
        f"latest {shared.topic} news",
        f"{shared.topic} SEC filings",
        f"{shared.topic} analyst reports",
    ]
    typer.echo(f"    queries: {len(shared.queries)}")


@step("fetch_news", retry=RetryPolicy(attempts=3, delay=0.5))
async def fetch_news(shared: PipelineState) -> None:
    await asyncio.sleep(0.30 + random.uniform(0, 0.05))
    shared.news = _NEWS
    typer.echo(f"    news: {len(shared.news)} items")


@step("fetch_filings", retry=RetryPolicy(attempts=3, delay=0.5))
async def fetch_filings(shared: PipelineState) -> None:
    await asyncio.sleep(0.30 + random.uniform(0, 0.05))
    if _fail_filings:
        raise RuntimeError("SEC EDGAR rate limit — try again")
    shared.filings = _FILINGS
    typer.echo(f"    filings: {len(shared.filings)} items")


@step("fetch_reports", retry=RetryPolicy(attempts=3, delay=0.5))
async def fetch_reports(shared: PipelineState) -> None:
    await asyncio.sleep(0.30 + random.uniform(0, 0.05))
    shared.reports = _REPORTS
    typer.echo(f"    reports: {len(shared.reports)} items")


@step("merge")
async def merge(shared: PipelineState) -> None:
    await asyncio.sleep(0.02)
    shared.findings = shared.news + shared.filings + shared.reports
    typer.echo(f"    findings: {len(shared.findings)} total")


@step("summarise")
async def summarise(shared: PipelineState) -> None:
    lines = [f"Topic: {shared.topic}", f"Generated: {datetime.now():%Y-%m-%d %H:%M}", ""]
    lines += [f"• {f}" for f in shared.findings]
    shared.brief = "\n".join(lines)
    out = _LOGS_DIR / "brief_split.txt"
    out.write_text(shared.brief)
    typer.echo(f"    brief → {out}")


# ── Pipeline ──────────────────────────────────────────────────────────────────


@pipeline()
def build() -> Flow:
    return Flow(
        plan >> split(fetch_news, fetch_filings, fetch_reports) >> merge,
        merge >> summarise,
        state_cls=PipelineState,
        on_node_start=lambda n: typer.echo(f"▸ {n}"),
        on_node_skip=lambda n: typer.echo(f"↩ skip {n}"),
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


@_app.command()
def run(
    fail_filings: Annotated[
        bool, typer.Option("--fail-filings", help="Simulate a fetch_filings failure")
    ] = False,
) -> None:
    """Execute the pipeline from the start node."""
    global _fail_filings
    _fail_filings = fail_filings
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
