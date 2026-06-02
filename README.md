# TideLock

**Zero-infrastructure workflow orchestration with automatic checkpointing.**

TideLock lets you write data pipelines as DAGs of Python steps. Every step is
automatically checkpointed — so when a step fails, you fix the bug and resume
from the failed node, skipping everything that already completed. No database,
no scheduler, no web UI, no cloud.

```bash
pip install tidelock
```

```python
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

## Why TideLock?

Every workflow orchestration tool asks you to set up infrastructure before you
can run a pipeline:

| Tool | Infrastructure needed |
|------|----------------------|
| **Prefect** | Server + database |
| **Metaflow** | AWS stack (S3, metadata service) |
| **Airflow** | Database + scheduler + web server |
| **Dagster** | Dagit web UI + database + daemon |
| **TideLock** | **Nothing** — `pip install` and run |

If you're building a data pipeline on a single machine — doing ETL, batch
processing, research analysis, or LLM inference — you shouldn't need a platform
team to get crash recovery. TideLock is the middle ground between "a raw Python
script that loses all progress on failure" and "a full orchestration platform
that needs a `docker-compose up`."

---

## How it works

### Steps

A step is a function that receives shared state and returns a routing string
(or `None` for the default path):

```python
@step("validate")
def validate(shared: State) -> str | None:
    if shared.records:
        return "has_data"
    return None
```

Steps can be sync or async:

```python
@step("fetch")
async def fetch(shared: State):
    async with httpx.AsyncClient() as client:
        shared.raw = await client.get("https://api.example.com/data")
```

### Graph DSL

Edges are built with two operators:

```python
Flow(
    fetch >> validate,                                # default transition
    (validate - "has_data") >> process,               # conditional transition
    (validate - "empty")    >> fallback,
    state_cls=State,
)
```

| Syntax | Meaning |
|--------|---------|
| `node_a >> node_b` | Default transition (step returns `None`) |
| `(node_a - "label") >> node_b` | Conditional transition (step returns `"label"`) |

The start node is inferred automatically — it's the unique node that appears
only as a source, never as a target. `Flow` raises `ValueError` if ambiguous.

### State

State is a Pydantic model. Two conveniences over plain `BaseModel`:

```python
class State(PipelineState):
    topic: str = "default"
    queries: list[str]          # auto default_factory=list
    sources: dict[str, str]     # auto default_factory=dict
    results: pd.DataFrame | None = None
```

1. **Bare collections** — `list`, `dict`, `set` get automatic `default_factory`.
2. **Field references** — `State.queries` returns a `FieldRef` for use with
   `.map()` and similar APIs, avoiding magic strings.

After every step the full state is serialized automatically:
- Plain Python values → msgpack
- `pd.DataFrame` fields → parquet
- `None` fields are skipped

### Checkpointing

Checkpoints are stored under `.pipeline_runs/<pid>/<node_name>/`:

```
.pipeline_runs/
  run_20260602_091500/
    metadata.json              # node order + status
    fetch/
      _action.msgpack          # routing value
      records.msgpack          # per-field checkpoint
    process/
      _action.msgpack
      records.msgpack
```

On `resume`, completed nodes are skipped and their checkpoints loaded to
restore state. The source run is **never modified** — a new run directory
is created and pre-populated with checkpoints up to the chosen node.

### Concurrent steps (`split`)

Run independent branches in parallel:

```python
Flow(
    plan >> split(fetch_news, fetch_filings, fetch_reports) >> merge,
    state_cls=State,
)
```

- Branches run via `asyncio.gather` — wall time equals the slowest branch.
- Each branch checkpoints independently.
- On resume, completed branches are skipped and failed ones re-run.

### Retry policies

```python
# Shorthand
@step("gather", retries=3, retry_delay=2.0)
async def gather(shared: State): ...

# Full control
@step("gather", retry=RetryPolicy(
    attempts=4,
    delay=1.0,
    backoff=2.0,          # delay doubles each attempt
    jitter=0.25,          # ±25% random spread
    on=RateLimitError,    # only retry this exception
))
async def gather(shared: State): ...
```

---

## CLI

### `run`

Execute the pipeline from the start node:

```bash
python pipeline.py run
```

### `resume <pid>`

Branch a previous run into a new run, skipping completed nodes:

```bash
python pipeline.py resume run_20260602_091500
```

With `--from-node` / `-f` to skip the interactive picker:

```bash
python pipeline.py resume run_20260602_091500 --from-node validate
```

### `inspect [pid]`

Open the Textual TUI to browse node state with regex search:

```bash
python pipeline.py inspect
python pipeline.py inspect run_20260602_091500
```

The TUI shows completed (green) and failed (red) nodes. Selecting a node
displays its state — JSON for msgpack fields, a sortable table for parquet
fields. Press `/` to search, `Esc` to clear, `q` to quit.

---

## Requirements

Python 3.12+

| Package | Role |
|---------|------|
| `pydantic` | State schema and validation |
| `msgpack` | Fast binary serialization |
| `pandas` + `pyarrow` | DataFrame serialization (parquet) |
| `typer` | CLI |
| `textual` | TUI inspector |
| `questionary` | Interactive node picker |

---

## Comparison

| | TideLock | Prefect | Metaflow | Airflow |
|---|---|---|---|---|
| Setup | `pip install` | Server + DB | AWS stack | DB + scheduler + web UI |
| Checkpointing | Per-step, file-based | Task retries only | Per-step, S3 | None |
| Resume | `resume <pid> -f <node>` | Manual retry | `--origin-run-id` | None |
| State inspection | Terminal TUI | Web UI | Web UI + CLI | Web UI |
| Concurrent branches | `split()` via asyncio | Task runners | `@parallel` | `PythonOperator` pools |
| DataFrame support | Native (parquet) | Via XCom | Native (S3) | Via XCom |
| Scheduling | No | Yes | No | Yes |
| Distributed execution | No | Yes | Yes | Yes |
| Lines of code | ~1,200 | ~200,000+ | ~150,000+ | ~300,000+ |

TideLock is not a replacement for Prefect/Airflow in production — it's for
the stage *before* that, when you're iterating on a pipeline locally and
don't want to stand up infrastructure just to survive a crash.

---

## Examples

| File | What it shows |
|------|--------------|
| `examples/split_pipeline.py` | Concurrent branches with `split()`, retry policies, resume on partial failure |

Run the example:

```bash
cd TideLock
uv run python examples/split_pipeline.py run
uv run python examples/split_pipeline.py inspect
```
