from __future__ import annotations

import asyncio
import glob
import json
import os
import random
import re
import shutil
import sys
import time
import traceback
import typing
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeVar

import msgpack
import pandas as pd
import questionary
import typer
from pydantic import BaseModel, Field
from pydantic._internal._model_construction import ModelMetaclass

T = TypeVar("T")

RUNS_DIR = "./.pipeline_runs"
_PID_PATTERN = re.compile(r"^run_\d{8}_\d{6}$")
_flow_generator: Callable[[], Flow] | None = None

# Collection types that get an automatic default_factory when left bare
_COLLECTION_ORIGINS = (list, dict, set)


def _validate_pid(pid: str) -> str:
    """Validate that *pid* matches the expected ``run_YYYYMMDD_HHMMSS`` format.

    Raises ``typer.Exit`` (exit code 1) if the format is invalid, which
    prevents path-traversal attacks via user-supplied PID values.
    """
    if not _PID_PATTERN.match(pid):
        typer.secho(
            f"Error: Invalid run PID '{pid}'. Expected format: run_YYYYMMDD_HHMMSS.",
            fg=typer.colors.RED,
        )
        raise typer.Exit(code=1)
    return pid


# --- 0. PIPELINE STATE ---


class FieldRef:
    """A reference to a named field on a PipelineState subclass.

    Produced by attribute access on the class itself::

        State.queries   # → FieldRef("queries")
        State.sources   # → FieldRef("sources")

    Used by .map() to avoid magic strings.
    """

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"FieldRef({self.name!r})"


def _collection_factory(annotation: str | type) -> type | None:
    """Return the collection factory for an annotation, or None if not a collection.

    Handles both real type objects (``list[str]``) and stringified annotations
    produced by ``from __future__ import annotations`` (``"list[str]"``).
    """
    if isinstance(annotation, str):
        s = annotation.strip()
        for factory, prefix in ((list, "list["), (dict, "dict["), (set, "set[")):
            if s == factory.__name__ or s.startswith(prefix):
                return factory
        return None
    origin = typing.get_origin(annotation)
    return origin if origin in _COLLECTION_ORIGINS else None


class _PipelineStateMeta(ModelMetaclass):
    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> _PipelineStateMeta:
        annotations = namespace.get("__annotations__", {})

        for field_name, annotation in annotations.items():
            # Skip private/dunder fields and ones that already have a default
            if field_name.startswith("_") or field_name in namespace:
                continue
            factory = _collection_factory(annotation)
            if factory is not None:
                namespace[field_name] = Field(default_factory=factory)

        return super().__new__(mcs, name, bases, namespace, **kwargs)

    def __getattr__(cls, name: str) -> FieldRef:
        # Walk the MRO and check __pydantic_fields__ directly via __dict__ to
        # avoid calling cls.model_fields, which is a descriptor that calls
        # getattr(cls, '__pydantic_fields__') and re-enters __getattr__.
        for klass in cls.__mro__:
            if name in klass.__dict__.get("__pydantic_fields__", {}):
                return FieldRef(name)
        raise AttributeError(f"{cls.__name__!r} has no field {name!r}")


class PipelineState(BaseModel, metaclass=_PipelineStateMeta):
    """Base class for TideLock pipeline state.

    Two conveniences over plain ``BaseModel``:

    * Bare collection annotations get an automatic ``default_factory``::

        class State(PipelineState):
            items: list[str]          # equivalent to Field(default_factory=list)
            counts: dict[str, int]    # equivalent to Field(default_factory=dict)

    * Field references work as class attributes, for use with ``.map()``::

        State.items    # → FieldRef("items")
        State.counts   # → FieldRef("counts")
    """


# --- 1. RETRY POLICY ---


@dataclass
class RetryPolicy:
    """Retry policy for a pipeline step.

    attempts : total number of tries (so retries=3 → attempts=4).
    delay    : initial wait in seconds before the second attempt.
    backoff  : multiplier applied to delay after each failure (1.0 = fixed).
    jitter   : random ±fraction of the computed delay (0.0 = no jitter).
    on       : exception class or tuple of classes to catch and retry;
               anything else propagates immediately without consuming budget.
    """

    attempts: int = 1
    delay: float = 1.0
    backoff: float = 2.0
    jitter: float = 0.0
    on: type[Exception] | tuple[type[Exception], ...] = field(default=Exception)


# --- 1. GRAPH PRIMITIVES ---


@dataclass
class Edge:
    """A directed edge between two nodes in a pipeline graph.

    May carry an optional *condition* label; if set, the edge is only
    traversed when the source step returns that exact label.
    """

    source: Node
    target: Node
    condition: str | None = None


@dataclass
class SplitEdge:
    """Fan-out edge produced by the ``split()`` helper.

    Connects a single source node to multiple target nodes that will
    be executed concurrently.
    """

    source: Node
    targets: list[Node]


@dataclass
class ConditionalBranch:
    """Intermediate produced by ``node - "label"`` in the DSL.

    Combining with ``>> target`` produces a conditional ``Edge``.
    """

    node: Node
    label: str

    def __rshift__(self, other: Node) -> Edge:
        return Edge(self.node, other, condition=self.label)


class Node:
    """A single step in a pipeline graph, wrapping a callable + retry config."""

    def __init__(
        self,
        name: str,
        func: Callable[..., Any],
        retry: RetryPolicy | None = None,
    ) -> None:
        self.name = name
        self.func = func
        self.retry = retry
        self.transitions: dict[str, Node] = {}
        self.split_targets: list[Node] = []

    def __rshift__(self, other: Any) -> Any:
        if isinstance(other, SplitGroup):
            other._source = self
            return other
        return Edge(self, other)

    def __sub__(self, label: str) -> ConditionalBranch:
        return ConditionalBranch(self, label)


class SplitGroup:
    """Returned by ``split()``; participates in the ``>>`` DSL.

    When used in a ``Flow`` edge chain, produces fan-out edges so that
    all member nodes run concurrently via ``asyncio.gather``.
    """

    def __init__(self, *nodes: Node) -> None:
        self.nodes = list(nodes)
        self._source: Node | None = None

    def __rshift__(self, target: Node) -> list[Edge | SplitEdge]:
        edges: list[Edge | SplitEdge] = []
        if self._source is not None:
            edges.append(SplitEdge(self._source, list(self.nodes)))
        for node in self.nodes:
            edges.append(Edge(node, target))
        return edges


def split(*nodes: Node) -> SplitGroup:
    """Declare concurrent step execution in a Flow edge chain.

    Example::

        Flow(
            plan >> split(fetch_a, fetch_b, fetch_c) >> merge,
            state_cls=State,
        )

    All steps inside split() run concurrently via asyncio.gather.
    Each step should write to distinct fields on shared state to
    avoid conflicts. Each step is checkpointed independently, so
    partial failures resume correctly.
    """
    return SplitGroup(*nodes)


class Flow:
    """A directed acyclic graph of pipeline steps with checkpointing support.

    Constructed via the ``>>`` / ``-`` DSL::

        Flow(
            fetch >> validate,
            (validate - "has_data") >> process,
            state_cls=State,
        )
    """

    def __init__(
        self,
        *edge_args: Any,
        state_cls: type[BaseModel],
        on_node_start: Callable[[str], None] | None = None,
        on_node_skip: Callable[[str], None] | None = None,
    ) -> None:
        if not edge_args:
            raise ValueError("Flow requires at least one edge")

        # Flatten: split() returns a list of edges; plain >> returns a single Edge
        edges: list[Edge | SplitEdge] = []
        for arg in edge_args:
            if isinstance(arg, list):
                edges.extend(arg)
            else:
                edges.append(arg)

        all_sources: set[Node] = set()
        all_targets: set[Node] = set()

        for edge in edges:
            if isinstance(edge, SplitEdge):
                edge.source.split_targets = list(edge.targets)
                all_sources.add(edge.source)
                all_targets.update(edge.targets)
            else:
                key = edge.condition or "default"
                edge.source.transitions[key] = edge.target
                all_sources.add(edge.source)
                all_targets.add(edge.target)

        starts = all_sources - all_targets
        if len(starts) != 1:
            raise ValueError(f"Expected exactly one start node, found {len(starts)}")

        self.start = starts.pop()
        self.state_cls = state_cls
        self.node_order = _ordered_node_names(self.start)
        self.on_node_start = on_node_start or (
            lambda name: typer.echo(f"Running node: '{name}'...")
        )
        self.on_node_skip = on_node_skip or (
            lambda name: typer.echo(f"[RESUME] Skipping node '{name}'...")
        )

    def _run_async(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine on a persistent event loop, creating one if needed."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                raise RuntimeError
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)

    def run(self, mode: str, pid: str, from_node: str | None = None) -> None:
        save_run_metadata(pid, self.node_order)
        run_started_at = datetime.now()
        try:
            self._run_async(self._execute(mode, pid, from_node))
        except Exception:
            finished_at = datetime.now()
            update_run_metadata(
                pid,
                status="failed",
                completed_at=finished_at.isoformat(),
                duration_s=round((finished_at - run_started_at).total_seconds(), 3),
            )
            traceback.print_exc()
            if mode == "resume":
                typer.secho(
                    "\n💡 Hint: This error likely means a step accessed state that wasn't"
                    "\n   properly restored from the previous run's checkpoint. CSV"
                    "\n   serialization loses dtype info (e.g. datetimes become strings),"
                    "\n   so columns loaded from checkpoints may have different types than"
                    "\n   expected. Add explicit type conversions in the failing step to"
                    "\n   make it robust against resume.",
                    fg=typer.colors.YELLOW,
                )
            sys.exit(1)
        finished_at = datetime.now()
        update_run_metadata(
            pid,
            status="completed",
            completed_at=finished_at.isoformat(),
            duration_s=round((finished_at - run_started_at).total_seconds(), 3),
        )

    async def _execute(self, mode: str, pid: str, from_node: str | None) -> None:
        """Main execution loop — handles sequential steps and split groups."""
        shared = self.state_cls()
        current_node: Node | None = self.start
        force_run = False

        while current_node:
            if mode == "resume" and from_node and current_node.name == from_node:
                force_run = True

            action, ran = await self._run_one(pid, current_node, shared, mode, force_run)
            if ran:
                force_run = True

            if current_node.split_targets:
                join_node = await self._run_split(pid, current_node, shared, mode, force_run)
                current_node = join_node
            elif action in current_node.transitions:
                current_node = current_node.transitions[action]
            else:
                current_node = current_node.transitions.get("default")

    async def _run_one(
        self, pid: str, node: Node, shared: BaseModel, mode: str, force_run: bool
    ) -> tuple[str, bool]:
        """Run or skip a single node. Returns (action, was_run)."""
        node_dir = os.path.join(RUNS_DIR, pid, node.name)

        if (
            mode == "resume"
            and not force_run
            and os.path.exists(os.path.join(node_dir, "_action.msgpack"))
        ):
            self.on_node_skip(node.name)
            action = load_node_checkpoint(pid, node.name, shared)
            return action, False

        self.on_node_start(node.name)
        started_at = datetime.now()
        try:
            result = node.func(shared)
            if asyncio.iscoroutine(result):
                result = await result
            action = result if result is not None else "default"
        except Exception as exc:
            completed_at = datetime.now()
            save_step_meta(
                pid,
                node.name,
                {
                    "status": "failed",
                    "started_at": started_at.isoformat(),
                    "completed_at": completed_at.isoformat(),
                    "duration_s": round((completed_at - started_at).total_seconds(), 3),
                    "retry_attempts": getattr(node.func, "_attempts_taken", 1),
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            )
            save_node_state(pid, node.name, shared)
            raise

        completed_at = datetime.now()
        save_step_meta(
            pid,
            node.name,
            {
                "status": "completed",
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_s": round((completed_at - started_at).total_seconds(), 3),
                "retry_attempts": getattr(node.func, "_attempts_taken", 1),
                "error_type": None,
                "error_message": None,
            },
        )
        save_node_checkpoint(pid, node.name, shared, action)
        return action, True

    async def _run_split(
        self, pid: str, source: Node, shared: BaseModel, mode: str, force_run: bool
    ) -> Node | None:
        """Run all split targets concurrently. Returns the join node."""
        results = await asyncio.gather(
            *[self._run_one(pid, node, shared, mode, force_run) for node in source.split_targets],
            return_exceptions=True,
        )

        # Re-raise first exception after all branches have had a chance to run
        for exc in results:
            if isinstance(exc, Exception):
                raise exc

        # The join node is the default transition of any split target
        for node in source.split_targets:
            join = node.transitions.get("default")
            if join:
                return join
        return None


# --- 2. STATE PERSISTENCE HELPERS ---
def _ordered_node_names(start: Node) -> list[str]:
    """BFS traversal from *start* returning node names in execution order."""
    order: list[str] = []
    seen: set[str] = set()
    queue = [start]
    while queue:
        node = queue.pop(0)
        if node.name in seen:
            continue
        seen.add(node.name)
        order.append(node.name)
        for split_node in node.split_targets:
            queue.append(split_node)
        for action in sorted(node.transitions):
            queue.append(node.transitions[action])
    return order


def save_run_metadata(pid: str, nodes: list[str]) -> None:
    """Write run metadata (node order + status) to ``metadata.json``.

    If the file already exists (e.g. on resume) this is a no-op.
    """
    pid_dir = os.path.join(RUNS_DIR, pid)
    os.makedirs(pid_dir, exist_ok=True)
    metadata_path = os.path.join(pid_dir, "metadata.json")
    if os.path.exists(metadata_path):
        return
    with open(metadata_path, "w") as f:
        json.dump(
            {
                "nodes": nodes,
                "status": "running",
                "started_at": datetime.now().isoformat(),
                "completed_at": None,
                "duration_s": None,
            },
            f,
            indent=2,
        )


def update_run_metadata(pid: str, **fields: Any) -> None:
    """Patch specific fields in ``metadata.json``."""
    metadata_path = os.path.join(RUNS_DIR, pid, "metadata.json")
    if not os.path.exists(metadata_path):
        return
    with open(metadata_path) as f:
        data = json.load(f)
    data.update(fields)
    with open(metadata_path, "w") as f:
        json.dump(data, f, indent=2)


def load_run_metadata(pid: str) -> list[str] | None:
    """Return the ordered node list for a run, or None if metadata is missing."""
    metadata_path = os.path.join(RUNS_DIR, pid, "metadata.json")
    if not os.path.exists(metadata_path):
        return None
    with open(metadata_path) as f:
        data = json.load(f)
    nodes = data.get("nodes")
    if not isinstance(nodes, list):
        return None
    return nodes


def load_run_summary(pid: str) -> dict | None:
    """Return the full metadata dict for a run, or None if missing."""
    metadata_path = os.path.join(RUNS_DIR, pid, "metadata.json")
    if not os.path.exists(metadata_path):
        return None
    with open(metadata_path) as f:
        return json.load(f)


def save_node_state(pid: str, node_name: str, shared: BaseModel) -> None:
    """Serialize all non-None state fields for a node.

    Does not write the routing action (use ``save_node_checkpoint`` for that).
    """
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    os.makedirs(node_dir, exist_ok=True)
    for key in shared.model_fields:
        value = getattr(shared, key)
        if value is None:
            continue
        if isinstance(value, pd.DataFrame):
            value.to_csv(os.path.join(node_dir, f"{key}.csv"), index=True)
        else:
            with open(os.path.join(node_dir, f"{key}.msgpack"), "wb") as f:
                f.write(msgpack.packb(value, use_bin_type=True))  # type: ignore[arg-type]


def save_node_checkpoint(pid: str, node_name: str, shared: BaseModel, action: str) -> None:
    """Persist both the routing action and the full node state to disk."""
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    os.makedirs(node_dir, exist_ok=True)
    with open(os.path.join(node_dir, "_action.msgpack"), "wb") as f:
        f.write(msgpack.packb(action, use_bin_type=True))  # type: ignore[arg-type]
    save_node_state(pid, node_name, shared)


def load_node_checkpoint(pid: str, node_name: str, shared: BaseModel) -> str:
    """Restore state from disk for a completed node.

    Returns the routing action that was persisted.
    """
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    for csv_file in glob.glob(os.path.join(node_dir, "*.csv")):
        key = os.path.splitext(os.path.basename(csv_file))[0]
        setattr(shared, key, pd.read_csv(csv_file, index_col=0))
    for m_file in glob.glob(os.path.join(node_dir, "*.msgpack")):
        key = os.path.splitext(os.path.basename(m_file))[0]
        if key == "_action":
            continue
        with open(m_file, "rb") as f:
            setattr(shared, key, msgpack.unpackb(f.read(), raw=False))

    with open(os.path.join(node_dir, "_action.msgpack"), "rb") as f:
        action: str = msgpack.unpackb(f.read(), raw=False)
        return action


def save_step_meta(pid: str, node_name: str, meta: dict[str, Any]) -> None:
    """Persist step execution metadata (timing, status, errors) to disk."""
    node_dir = os.path.join(RUNS_DIR, pid, node_name)
    os.makedirs(node_dir, exist_ok=True)
    with open(os.path.join(node_dir, "_meta.msgpack"), "wb") as f:
        f.write(msgpack.packb(meta, use_bin_type=True))  # type: ignore[arg-type]


def load_step_meta(pid: str, node_name: str) -> dict[str, Any] | None:
    """Load step execution metadata from disk, or None if unavailable."""
    path = os.path.join(RUNS_DIR, pid, node_name, "_meta.msgpack")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return msgpack.unpackb(f.read(), raw=False)


def node_has_checkpoint(pid: str, node_name: str) -> bool:
    """Return True when a checkpoint file exists for *node_name* in *pid*."""
    return os.path.exists(os.path.join(RUNS_DIR, pid, node_name, "_action.msgpack"))


def branch_run(source_pid: str, from_node: str) -> str:
    """Create a new run from *source_pid*, copying checkpoints up to *from_node*.

    The source run is never modified. The new run gets its own PID and
    starts with state restored from the completed nodes before *from_node*.
    """
    _validate_pid(source_pid)
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
    """Return node names for *pid*, optionally filtering to checkpointed ones."""
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
    """Show an interactive ``questionary`` list of nodes and return the selection."""
    _validate_pid(pid)
    nodes = list_run_nodes(pid, checkpointed_only=checkpointed_only)
    if not nodes:
        typer.echo("⚠️ No node serialization tracking folders discovered for this run.")
        return None

    order = load_run_metadata(pid) or nodes
    index_by_name = {name: idx for idx, name in enumerate(order)}

    choices = []
    for node in nodes:
        meta = load_step_meta(pid, node)
        has_checkpoint = node_has_checkpoint(pid, node)

        if meta and meta.get("status") == "failed":
            name_style = "class:failed"
        elif has_checkpoint:
            name_style = "class:completed"
        else:
            name_style = "class:pending"

        duration = meta.get("duration_s") if meta else None
        duration_str = f"  {duration:.2f}s" if duration is not None else ""

        choices.append(
            questionary.Choice(
                title=[
                    (name_style, f"{index_by_name[node]}.{node}"),
                    ("class:duration", duration_str),
                ],
                value=node,
            )
        )

    style = questionary.Style(
        [
            ("completed", "fg:ansigreen bold"),
            ("failed", "fg:ansired bold"),
            ("pending", "fg:ansiyellow"),
            ("duration", "fg:#666666"),
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


def step(
    name: str,
    *,
    retries: int = 0,
    retry_delay: float = 1.0,
    retry: RetryPolicy | None = None,
) -> Callable[[Callable[..., Any]], Node]:
    """Declare a pipeline step.

    Simple retry shorthand::

        @step("gather", retries=3, retry_delay=2.0)
        async def gather(shared): ...

    Full control via RetryPolicy::

        @step("gather", retry=RetryPolicy(attempts=4, delay=1.0, backoff=2.0, on=RateLimitError))
        async def gather(shared): ...

    ``retries=N`` is sugar for ``RetryPolicy(attempts=N+1, delay=retry_delay)``.
    """
    if retry is None and retries:
        retry = RetryPolicy(attempts=retries + 1, delay=retry_delay)

    def _warn(attempt: int, exc: Exception, wait: float, total: int) -> None:
        typer.secho(
            f"  Retrying {name} — attempt {attempt}/{total} — "
            f"{type(exc).__name__}: {exc}  (retry in {wait:.1f}s)",
            fg=typer.colors.YELLOW,
            err=True,
        )

    def _next_wait(current: float) -> float:
        assert retry is not None
        w = current * retry.backoff
        if retry.jitter:
            w += w * random.uniform(-retry.jitter, retry.jitter)
        return w

    def decorator(func: Callable[..., Any]) -> Node:
        if retry is None:
            return Node(name, func)

        if asyncio.iscoroutinefunction(func):
            return _build_async_retry(name, func, retry, _warn, _next_wait)

        return _build_sync_retry(name, func, retry, _warn, _next_wait)

    return decorator


def _build_async_retry(
    name: str,
    func: Callable[..., Any],
    policy: RetryPolicy,
    warn: Callable[[int, Exception, float, int], None],
    next_wait: Callable[[float], float],
) -> Node:
    """Wrap an async step function with retry logic."""

    async def wrapped(shared: Any) -> Any:
        wait = policy.delay
        for attempt in range(1, policy.attempts + 1):
            try:
                result = await func(shared)
                wrapped._attempts_taken = attempt
                return result
            except policy.on as exc:
                wrapped._attempts_taken = attempt
                if attempt == policy.attempts:
                    raise
                warn(attempt, exc, wait, policy.attempts)
                await asyncio.sleep(wait)
                wait = next_wait(wait)

    wrapped.__name__ = func.__name__
    wrapped._attempts_taken = 1
    return Node(name, wrapped, retry=policy)


def _build_sync_retry(
    name: str,
    func: Callable[..., Any],
    policy: RetryPolicy,
    warn: Callable[[int, Exception, float, int], None],
    next_wait: Callable[[float], float],
) -> Node:
    """Wrap a synchronous step function with retry logic."""

    def wrapped(shared: Any) -> Any:
        wait = policy.delay
        for attempt in range(1, policy.attempts + 1):
            try:
                result = func(shared)
                wrapped._attempts_taken = attempt
                return result
            except policy.on as exc:
                wrapped._attempts_taken = attempt
                if attempt == policy.attempts:
                    raise
                warn(attempt, exc, wait, policy.attempts)
                time.sleep(wait)
                wait = next_wait(wait)

    wrapped.__name__ = func.__name__
    wrapped._attempts_taken = 1
    return Node(name, wrapped, retry=policy)


def pipeline() -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator that registers a function as the pipeline graph constructor.

    The decorated function should return a :class:`Flow` instance.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        global _flow_generator
        _flow_generator = func
        return func

    return decorator


# --- 4. TYPER CLI ROUTER ---
cli = typer.Typer(help="TideLock: PocketFlow-Style Architecture", no_args_is_help=True)


def start_flow(mode: str, pid: str | None = None, from_node: str | None = None) -> None:
    pid = pid or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _validate_pid(pid)
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
    pid: str | None = typer.Argument(None, help="Run PID to resume from (defaults to most recent)"),
    from_node: str | None = typer.Option(
        None, "--from-node", "-f", help="The specific node name to start executing fresh from"
    ),
):
    """Branch a run from a chosen node into a new run (source run is left unchanged)."""
    if pid is None:
        pid = _most_recent_run()
        if pid is None:
            typer.secho("No runs found in .pipeline_runs/", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        typer.echo(f"Using most recent run: {pid}")

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


def _most_recent_run() -> str | None:
    """Return the most recently modified run directory under RUNS_DIR, or ``None``."""
    runs_path = os.path.join(RUNS_DIR)
    if not os.path.exists(runs_path):
        return None
    dirs = [d for d in os.listdir(runs_path) if os.path.isdir(os.path.join(runs_path, d))]
    if not dirs:
        return None
    return max(dirs, key=lambda d: os.path.getmtime(os.path.join(runs_path, d)))


@cli.command(name="inspect")
def inspect_pid(
    pid: str | None = typer.Argument(None, help="Run PID to inspect (defaults to most recent)"),
):
    """Interactively select a node via searchable list, then launch the Textual TUI viewer."""
    if pid is None:
        pid = _most_recent_run()
        if pid is None:
            typer.secho("No runs found in .pipeline_runs/", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        typer.echo(f"Using most recent run: {pid}")

    selected_node = prompt_select_node(
        pid,
        "Select a pipeline node to view its state context (Type to filter options):",
    )
    if not selected_node:
        return

    from tidelock.tui import NodeInspectorApp

    NodeInspectorApp(pid, selected_node).run()
