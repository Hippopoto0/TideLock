# TideLock

A lightweight pipeline checkpointing library built on a PocketFlow-style graph pattern. Define your pipeline as a DAG of named steps, run it, and get automatic per-step state snapshots — so you can resume from any node without re-running what already completed.

---

## Quick start

```bash
uv add tidelock  # or pip install tidelock
```

```python
# pipeline.py
from tidelock.engine import Flow, PipelineState, cli, pipeline, step

class State(PipelineState):
    records: list[dict]

@step("fetch")
def fetch(shared: State):
    shared.records = [{"id": 1, "value": 42}]

@step("process")
def process(shared: State):
    for r in shared.records:
        r["value"] *= 2

@pipeline()
def construct():
    return Flow(fetch >> process, state_cls=State)

if __name__ == "__main__":
    cli()
```

```bash
python pipeline.py run
python pipeline.py resume run_20260101_120000
python pipeline.py inspect
```

---

## Graph DSL

Edges are expressed with two operators:

| Syntax | Meaning |
|--------|---------|
| `node_a >> node_b` | Default transition — taken when the step returns `None` |
| `(node_a - "label") >> node_b` | Conditional transition — taken when the step returns `"label"` |

Pass all edges to `Flow`, along with `state_cls`:

```python
Flow(
    fetch >> validate,
    (validate - "high_volume") >> route_vip,
    (validate - "default")    >> route_standard,
    state_cls=State,
)
```

The start node is inferred automatically — it is the unique node that appears only as a source, never as a target. `Flow` raises `ValueError` if that is ambiguous.

A node can route back to itself. Return its own routing label (or `None` for the default) and wire the self-loop edge in the `Flow` constructor.

---

## Concurrent steps (`split`)

Use `split()` to run independent branches concurrently:

```python
from tidelock.engine import Flow, split, step

Flow(
    plan >> split(fetch_news, fetch_filings, fetch_reports) >> merge,
    state_cls=State,
)
```

All steps inside `split()` run concurrently via `asyncio.gather`. Execution continues to the merge node only after every branch completes.

**Checkpointing** — each branch checkpoints independently. On resume, completed branches are skipped and failed ones re-run:

```
first run:   fetch_news ✓   fetch_filings ✗   fetch_reports ✓
resume:      fetch_news ↩   fetch_filings ↺   fetch_reports ↩
```

**Shared state** — concurrent branches should write to distinct fields on `shared` to avoid conflicts. Python's asyncio is single-threaded so there are no data races, but two branches writing the same field will overwrite each other.

---

## Step functions

```python
@step("name")
def my_step(shared: MyState) -> str | None:
    ...
```

- The function receives the shared state object directly. Mutate it in place.
- Return a string to follow a conditional edge, or return `None` (or nothing) to follow the default edge.
- The name passed to `@step` is used for checkpoint directories and CLI output.

---

## State

Inherit from `PipelineState` (re-exported from `tidelock.engine`):

```python
from tidelock.engine import PipelineState

class State(PipelineState):
    topic: str = "default"
    queries: list[str]          # auto default_factory=list
    sources: dict[str, str]     # auto default_factory=dict
    results: pd.DataFrame | None = None
```

Two conveniences over plain `BaseModel`:

- **Bare collection annotations** (`list`, `dict`, `set`) automatically get `default_factory` — no `Field(default_factory=list)` boilerplate needed.
- **Field references** work as class attributes (`State.queries`, `State.sources`), used by `.map()` to avoid magic strings.

Every field is serialized automatically after each step:

- Plain Python values (lists, dicts, strings, numbers) → msgpack
- `pd.DataFrame` fields → parquet

Fields set to `None` are not written to disk. Checkpoints are stored under `.pipeline_runs/<pid>/<node_name>/`.

---

## Async steps

`@step` works with both `def` and `async def`:

```python
@step("fetch")
async def fetch(shared: State):
    async with httpx.AsyncClient() as client:
        shared.raw = (await client.get("https://example.com/api")).json()
```

TideLock detects coroutines and runs them via `asyncio.get_event_loop().run_until_complete()`.

If multiple async steps share a resource (such as a connection pool), create and install a persistent event loop **before** importing TideLock:

```python
import asyncio
asyncio.set_event_loop(asyncio.new_event_loop())

from tidelock.engine import Flow, cli, pipeline, step
```

---

## Retry policies

Steps can declare a retry policy for transient failures.

**Simple shorthand** — `retries` and `retry_delay`:

```python
@step("gather", retries=3, retry_delay=2.0)
async def gather(shared: State):
    ...
```

`retries=3` means up to 4 total attempts. Delays follow an exponential backoff: 2 s, 4 s, 8 s.

**Full control** — pass a `RetryPolicy` object:

```python
from tidelock.engine import RetryPolicy, step

@step("gather", retry=RetryPolicy(
    attempts=4,
    delay=1.0,
    backoff=2.0,          # delay doubles each attempt: 1 s → 2 s → 4 s
    jitter=0.25,          # ±25 % random spread to avoid thundering-herd
    on=RateLimitError,    # only retry this exception; others propagate immediately
))
async def gather(shared: State):
    ...
```

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `attempts` | `1` | Total tries (retries + 1) |
| `delay` | `1.0` | Wait in seconds before the second attempt |
| `backoff` | `2.0` | Multiplier applied to delay after each failure (`1.0` = fixed) |
| `jitter` | `0.0` | Random ±fraction added to each wait to spread retries |
| `on` | `Exception` | Exception class or tuple of classes to catch; everything else propagates immediately |

If all attempts are exhausted the original exception is re-raised. A step that eventually succeeds checkpoints once, as if it had run without retries — the retry loop is invisible to the resume system.

Failed attempts are logged to stderr:

```
⚠ gather  attempt 1/4 — RateLimitError: quota exceeded  (retry in 1.0s)
⚠ gather  attempt 2/4 — RateLimitError: quota exceeded  (retry in 2.0s)
⚠ gather  attempt 3/4 — RateLimitError: quota exceeded  (retry in 4.0s)
```

---

## Pipeline decorator

```python
@pipeline()
def construct():
    return Flow(...)
```

`@pipeline` registers the function as the factory that `cli()` calls before executing the graph. There must be exactly one `@pipeline`-decorated function in the module.

---

## CLI commands

Run `python pipeline.py <command>` (or whatever module contains your `cli()` call).

### `run`

Execute the pipeline from the start node.

```bash
python pipeline.py run
```

A new run ID (`run_YYYYMMDD_HHMMSS`) is created automatically.

### `resume <pid>`

Branch a previous run into a new run, skipping nodes that already completed.

```bash
python pipeline.py resume run_20260101_120000
```

With `--from-node` / `-f` to skip the interactive picker:

```bash
python pipeline.py resume run_20260101_120000 --from-node validate
```

Without `--from-node`, an interactive searchable list appears (completed nodes highlighted in green). The source run is never modified — a new run directory is created and pre-populated with checkpoints up to (but not including) the chosen node.

### `inspect [pid]`

Open the Textual TUI to browse node state. Defaults to the most recent run if no PID is given.

```bash
python pipeline.py inspect
python pipeline.py inspect run_20260101_120000
```

---

## Checkpointing

Checkpointing is automatic. After every step, `Flow.run()` writes the full state to:

```
.pipeline_runs/
  <pid>/
    metadata.json          # node order for the run
    <node_name>/
      _action.msgpack      # routing value returned by the step
      <field>.msgpack      # one file per non-None state field
      <field>.parquet      # DataFrames use parquet instead
```

On `resume`, nodes before the chosen branch point are skipped and their checkpoints are loaded to restore state; nodes at and after it are re-executed.

---

## TUI inspector

The inspector is a [Textual](https://github.com/Textualize/textual) terminal app.

- **Left sidebar** — lists all serialized variables for the selected node.
- **Right panel** — shows JSON (msgpack fields) or a `DataTable` (parquet fields).
- Press `/` to open a regex search bar. Matches are listed with snippets; selecting one jumps to and highlights the match in the text view.
- Press `Esc` to clear search. Press `q` to quit.

---

## Dependencies

| Package | Role |
|---------|------|
| `pydantic` | State schema and validation |
| `msgpack` | Fast binary serialization |
| `pandas` + `pyarrow` | DataFrame serialization (parquet) |
| `typer` | CLI |
| `textual` | TUI inspector |
| `questionary` | Interactive node picker |
