import glob
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime

import msgpack
import pandas as pd
import typer
from pydantic import BaseModel

RUNS_DIR = "./.pipeline_runs"
_flow_generator = None  # Holds the user's create_graph function


# --- 1. POCKETFLOW CORE ABSTRACTION CLONE ---
@dataclass
class Edge:
    source: "Node"
    target: "Node"
    condition: str | None = None


@dataclass
class ConditionalBranch:
    node: "Node"
    label: str

    def __rshift__(self, other: "Node") -> Edge:
        return Edge(self.node, other, condition=self.label)


class Node:
    def __init__(self, name: str, func):
        self.name = name
        self.func = func
        self.transitions = {}  # action_name -> Node

    def __rshift__(self, other: "Node") -> Edge:
        return Edge(self, other)

    def __sub__(self, label: str) -> ConditionalBranch:
        return ConditionalBranch(self, label)


class Flow:
    def __init__(self, *edges: Edge, state_cls: type[BaseModel]):
        if not edges:
            raise ValueError("Flow requires at least one edge")

        for edge in edges:
            key = edge.condition or "default"
            edge.source.transitions[key] = edge.target

        sources = {edge.source for edge in edges}
        targets = {edge.target for edge in edges}
        starts = sources - targets
        if len(starts) != 1:
            raise ValueError(f"Expected exactly one start node, found {len(starts)}")

        self.start = starts.pop()
        self.state_cls = state_cls
        self.node_order = _ordered_node_names(self.start)

    def run(self, mode: str, pid: str, from_node: str | None = None):
        save_run_metadata(pid, self.node_order)

        shared = self.state_cls()
        current_node = self.start
        force_run = False

        while current_node:
            node_dir = os.path.join(RUNS_DIR, pid, current_node.name)

            if mode == "resume" and from_node and current_node.name == from_node:
                force_run = True

            if (
                mode == "resume"
                and not force_run
                and os.path.exists(os.path.join(node_dir, "_action.msgpack"))
            ):
                typer.echo(
                    f"↩️  [RESUME] Skipping node '{current_node.name}' and restoring state schema..."
                )
                action = load_node_checkpoint(pid, current_node.name, shared)
            else:
                force_run = True
                typer.echo(f"🚀 Running node: '{current_node.name}'...")
                action = current_node.func(shared)
                if action is None:
                    action = "default"

                save_node_checkpoint(pid, current_node.name, shared, action)

            # Evaluate where the graph moves next
            if action in current_node.transitions:
                current_node = current_node.transitions[action]
            else:
                current_node = current_node.transitions.get("default")


# --- 2. STATE PERSISTENCE HELPERS ---
def _ordered_node_names(start: Node) -> list[str]:
    order: list[str] = []
    seen: set[str] = set()
    queue = [start]
    while queue:
        node = queue.pop(0)
        if node.name in seen:
            continue
        seen.add(node.name)
        order.append(node.name)
        for action in sorted(node.transitions):
            queue.append(node.transitions[action])
    return order


def save_run_metadata(pid: str, nodes: list[str]) -> None:
    pid_dir = os.path.join(RUNS_DIR, pid)
    os.makedirs(pid_dir, exist_ok=True)
    metadata_path = os.path.join(pid_dir, "metadata.json")
    if os.path.exists(metadata_path):
        return
    with open(metadata_path, "w") as f:
        json.dump({"nodes": nodes}, f, indent=2)


def load_run_metadata(pid: str) -> list[str] | None:
    metadata_path = os.path.join(RUNS_DIR, pid, "metadata.json")
    if not os.path.exists(metadata_path):
        return None
    with open(metadata_path) as f:
        data = json.load(f)
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return None
    return nodes


def save_node_checkpoint(pid, node_name, shared, action):
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    os.makedirs(node_dir, exist_ok=True)
    with open(os.path.join(node_dir, "_action.msgpack"), "wb") as f:
        f.write(msgpack.packb(action, use_bin_type=True))

    for key in shared.model_fields:
        value = getattr(shared, key)
        if value is None:
            continue

        if isinstance(value, pd.DataFrame):
            value.to_parquet(os.path.join(node_dir, f"{key}.parquet"))
        else:
            with open(os.path.join(node_dir, f"{key}.msgpack"), "wb") as f:
                f.write(msgpack.packb(value, use_bin_type=True))


def load_node_checkpoint(pid, node_name, shared):
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    for p_file in glob.glob(os.path.join(node_dir, "*.parquet")):
        key = os.path.splitext(os.path.basename(p_file))[0]
        setattr(shared, key, pd.read_parquet(p_file))
    for m_file in glob.glob(os.path.join(node_dir, "*.msgpack")):
        key = os.path.splitext(os.path.basename(m_file))[0]
        if key == "_action":
            continue
        with open(m_file, "rb") as f:
            setattr(shared, key, msgpack.unpackb(f.read(), raw=False))

    with open(os.path.join(node_dir, "_action.msgpack"), "rb") as f:
        return msgpack.unpackb(f.read(), raw=False)


def node_has_checkpoint(pid: str, node_name: str) -> bool:
    return os.path.exists(os.path.join(RUNS_DIR, pid, node_name, "_action.msgpack"))


def branch_run(source_pid: str, from_node: str) -> str:
    order = load_run_metadata(source_pid)
    if not order:
        typer.secho(
            f"❌ Error: Run '{source_pid}' has no metadata.json; cannot branch.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    if from_node not in order:
        typer.secho(
            f"❌ Error: Node '{from_node}' is not in this run's graph.", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)

    new_pid = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    new_dir = os.path.join(RUNS_DIR, new_pid)
    os.makedirs(new_dir, exist_ok=True)

    shutil.copy2(
        os.path.join(RUNS_DIR, source_pid, "metadata.json"), os.path.join(new_dir, "metadata.json")
    )

    for node_name in order[: order.index(from_node)]:
        if not node_has_checkpoint(source_pid, node_name):
            continue
        shutil.copytree(
            os.path.join(RUNS_DIR, source_pid, node_name),
            os.path.join(new_dir, node_name),
        )

    typer.echo(f"↪️  Branched '{source_pid}' → '{new_pid}' (resume from '{from_node}')")
    return new_pid


def list_run_nodes(pid: str, *, checkpointed_only: bool = True) -> list[str]:
    pid_dir = os.path.join(RUNS_DIR, pid)
    if not os.path.exists(pid_dir):
        typer.secho(f"❌ Error: PID '{pid}' not found.", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    checkpointed = {d for d in os.listdir(pid_dir) if os.path.isdir(os.path.join(pid_dir, d))}
    order = load_run_metadata(pid)
    if order:
        if checkpointed_only:
            return [name for name in order if name in checkpointed]
        return order
    return sorted(checkpointed)


def prompt_select_node(pid: str, message: str, *, checkpointed_only: bool = True) -> str | None:
    import questionary

    nodes = list_run_nodes(pid, checkpointed_only=checkpointed_only)
    if not nodes:
        typer.echo("⚠️ No node serialization tracking folders discovered for this run.")
        return None

    order = load_run_metadata(pid) or nodes
    index_by_name = {name: idx for idx, name in enumerate(order)}

    choices = [
        questionary.Choice(
            title=[
                (
                    "class:completed" if node_has_checkpoint(pid, node) else "class:pending",
                    f"{index_by_name[node]}.{node}",
                )
            ],
            value=node,
        )
        for node in nodes
    ]

    style = questionary.Style(
        [
            ("completed", "fg:ansigreen bold"),
            ("pending", "fg:ansired"),
        ]
    )

    return questionary.select(
        message,
        choices=choices,
        style=style,
        use_search_filter=True,
        use_jk_keys=False,
    ).ask()


# --- 3. THE FRAMEWORK DECORATORS ---
def step(name: str):
    def decorator(func):
        return Node(name, func)

    return decorator


def pipeline():
    def decorator(func):
        global _flow_generator
        _flow_generator = func
        return func

    return decorator


# --- 4. TYPER CLI ROUTER ---
cli = typer.Typer(help="TideLock: PocketFlow-Style Architecture", no_args_is_help=True)


def start_flow(mode: str, pid: str | None = None, from_node: str | None = None):
    pid = pid or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if mode == "resume" and not os.path.exists(os.path.join(RUNS_DIR, pid)):
        typer.secho(f"❌ Error: PID '{pid}' does not exist.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if _flow_generator:
        flow_obj = _flow_generator()
        typer.echo(f"\n--- Executing Graph Pipeline | PID: {pid} | Mode: {mode} ---")
        flow_obj.run(mode, pid, from_node)
        typer.echo("🎉 Graph sequence completed.")
    else:
        typer.secho(
            "❌ Error: No @pipeline graph construction function defined.", fg=typer.colors.RED
        )


@cli.command()
def run():
    """Execute graph from the start node."""
    start_flow("run")


@cli.command()
def resume(
    pid: str = typer.Argument(..., help="The explicit PID folder to recover from"),
    from_node: str | None = typer.Option(
        None, "--from-node", "-f", help="The specific node name to start executing fresh from"
    ),
):
    """Branch a run from a chosen node into a new run (source run is left unchanged)."""
    if from_node is None:
        from_node = prompt_select_node(
            pid,
            "Select a node to resume from (green=completed, red=not run):",
            checkpointed_only=False,
        )
        if not from_node:
            return

    new_pid = branch_run(pid, from_node)
    start_flow("resume", new_pid, from_node)


@cli.command()
def inspect(pid: str = typer.Argument(..., help="The explicit Run PID to view")):
    """Interactively select a node via searchable list, then launch the Textual TUI viewer."""
    selected_node = prompt_select_node(
        pid,
        "Select a pipeline node to view its state context (Type to filter options):",
    )
    if not selected_node:
        return

    from tui import NodeInspectorApp

    NodeInspectorApp(pid, selected_node).run()
