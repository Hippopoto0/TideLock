import json
import os
import re
from dataclasses import dataclass

import msgpack
import pandas as pd
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Input, Label, ListItem, ListView, Static, TextArea
from textual.document._document import Selection

from tidelock.engine import RUNS_DIR, load_step_meta

SEARCH_CONTEXT_CHARS = 80
MAX_SEARCH_MATCHES = 100


@dataclass
class SearchMatch:
    index: int
    start: int
    end: int
    group: str
    snippet: str


def offset_to_line_col(text: str, offset: int) -> tuple[int, int]:
    before = text[:offset]
    line = before.count("\n")
    last_nl = before.rfind("\n")
    col = offset if last_nl == -1 else offset - last_nl - 1
    return line, col


def find_regex_matches(
    text: str, pattern: str, context_chars: int = SEARCH_CONTEXT_CHARS
) -> list[SearchMatch]:
    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error:
        return []

    matches: list[SearchMatch] = []
    for found in regex.finditer(text):
        if len(matches) >= MAX_SEARCH_MATCHES:
            break

        start = max(0, found.start() - context_chars)
        end = min(len(text), found.end() + context_chars)
        snippet = text[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."

        matches.append(
            SearchMatch(
                index=len(matches),
                start=found.start(),
                end=found.end(),
                group=found.group(),
                snippet=snippet,
            )
        )

    return matches


class NodeInspectorApp(App):
    CSS = """
    Screen { background: #1a1a1a; }
    #sidebar { width: 32; background: #262626; border-right: tall #333; }
    #viewer { padding: 1; }
    #search_input { display: none; margin-bottom: 1; }
    #search_input.visible { display: block; }
    #match_list { display: none; height: 12; max-height: 14; border: tall #333; margin-bottom: 1; }
    #match_list.visible { display: block; }
    ListItem { padding: 1; }
    ListItem:hover { background: #34495e; }
    Label { text-style: bold; margin-bottom: 1; color: #1abc9c; }
    #df_view, #text_view { display: none; }
    #text_view.visible { display: block; height: 1fr; }
    #meta_strip { padding: 0 1; margin-bottom: 1; color: #aaaaaa; }
    """
    BINDINGS = [
        ("/", "focus_search", "Search"),
        ("escape", "clear_search", "Clear search"),
        ("q", "quit", "Exit Node Viewer"),
    ]

    def __init__(self, pid: str, node_name: str):
        super().__init__()
        self.pid = pid
        self.node_name = node_name
        self.node_dir = os.path.join(RUNS_DIR, pid, node_name)
        self.step_meta: dict | None = load_step_meta(pid, node_name)
        self.variables = []
        if os.path.exists(self.node_dir):
            # Exclude all internal files (underscore-prefixed)
            self.variables = sorted(
                f for f in os.listdir(self.node_dir) if not f.startswith("_")
            )
        self.file_by_var_id = {f"var-{i}": filename for i, filename in enumerate(self.variables)}
        self.current_text = ""
        self.view_kind: str | None = None
        self.current_data = None
        self.search_matches: list[SearchMatch] = []
        self.search_error: str | None = None

    def _format_meta(self) -> str:
        m = self.step_meta
        if not m:
            return ""

        status = m.get("status", "")
        if status == "completed":
            icon = "[bold green]✓[/bold green]"
        elif status == "failed":
            icon = "[bold red]✗[/bold red]"
        else:
            icon = "[dim]·[/dim]"

        duration = m.get("duration_s")
        duration_str = f"  {duration:.2f}s" if duration is not None else ""

        attempts = m.get("retry_attempts", 1)
        # Surface retry count only when more than one attempt was used
        retry_str = f"\n[yellow]⟳ attempt {attempts}[/yellow]" if attempts and attempts > 1 else ""

        error_type = m.get("error_type")
        error_msg = m.get("error_message") or ""
        error_str = f"\n[red]{error_type}[/red]" if error_type else ""
        if error_type and error_msg:
            # Truncate long messages to fit the sidebar
            truncated = error_msg[:26] + "…" if len(error_msg) > 27 else error_msg
            error_str += f"\n[dim]{truncated}[/dim]"

        return f"{icon}[dim]{duration_str}[/dim]{retry_str}{error_str}"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Label(f" Node: {self.node_name}")
                meta_markup = self._format_meta()
                if meta_markup:
                    yield Static(meta_markup, id="meta_strip")
                yield ListView(
                    *[
                        ListItem(Label(os.path.splitext(v)[0]), id=f"var-{i}")
                        for i, v in enumerate(self.variables)
                    ],
                    id="var_list",
                )
            with Vertical(id="viewer"):
                yield Label("Select an attribute/variable from the sidebar to inspect...")
                yield Input(
                    id="search_input",
                    placeholder="Regex search (Enter to find, Esc to cancel)",
                )
                yield ListView(id="match_list")
                yield DataTable(id="df_view")
                yield TextArea(id="text_view", read_only=True, show_line_numbers=True)
        yield Footer()

    def _hide_search(self) -> None:
        search = self.query_one("#search_input", Input)
        search.remove_class("visible")
        search.value = ""
        self.query_one("#match_list", ListView).remove_class("visible")
        self.search_matches = []
        self.search_error = None

    def _hide_text_view(self) -> None:
        text_view = self.query_one("#text_view", TextArea)
        text_view.remove_class("visible")
        text_view.text = ""

    @property
    def _content_language(self) -> str:
        return "json" if self.view_kind == "msgpack" else "text"

    def _show_text_content(self, content: str, *, language: str | None = None) -> None:
        table = self.query_one("#df_view", DataTable)
        text_view = self.query_one("#text_view", TextArea)
        table.display = False
        text_view.language = language or "text"
        text_view.text = content
        text_view.add_class("visible")

    def _show_default_view(self) -> None:
        table = self.query_one("#df_view", DataTable)
        table.clear(columns=True)
        self._hide_text_view()
        table.display = False

        if self.view_kind == "dataframe" and self.current_data is not None:
            df = self.current_data
            table.display = True
            table.add_columns(*df.columns)
            table.add_rows(df.values.tolist())
        elif self.view_kind == "msgpack" and self.current_data is not None:
            self._show_text_content(self.current_text, language=self._content_language)

    def _goto_match(self, match_index: int) -> None:
        if match_index < 0 or match_index >= len(self.search_matches):
            return

        match = self.search_matches[match_index]
        text_view = self.query_one("#text_view", TextArea)
        self._show_text_content(self.current_text, language=self._content_language)

        start = offset_to_line_col(self.current_text, match.start)
        end = offset_to_line_col(self.current_text, match.end)
        text_view.selection = Selection(start, end)
        text_view.move_cursor(start)
        text_view.scroll_cursor_visible(animate=False)

    async def _show_search_results(self, pattern: str) -> None:
        self.search_error = None
        try:
            re.compile(pattern, re.IGNORECASE)
        except re.error as exc:
            self.search_error = str(exc)
            self.search_matches = []
        else:
            self.search_matches = find_regex_matches(self.current_text, pattern)

        match_list = self.query_one("#match_list", ListView)
        await match_list.clear()
        match_list.remove_class("visible")

        if self.search_error:
            self._show_text_content(f"Invalid regex: {self.search_error}")
            return

        if not self.search_matches:
            self._show_text_content(f"No matches for /{pattern}/")
            return

        for match in self.search_matches:
            line, _ = offset_to_line_col(self.current_text, match.start)
            match_list.append(
                ListItem(
                    Label(f"{match.index}. {match.group!r}  (line {line + 1})"),
                    id=f"match-{match.index}",
                )
            )

        match_list.add_class("visible")
        self._show_text_content(self.current_text, language=self._content_language)

    def action_focus_search(self) -> None:
        if not self.current_text:
            return
        search = self.query_one("#search_input", Input)
        search.add_class("visible")
        search.focus()

    def action_clear_search(self) -> None:
        self._hide_search()
        self._show_default_view()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "search_input":
            return

        pattern = event.input.value.strip()
        if not pattern:
            self.action_clear_search()
            return

        await self._show_search_results(pattern)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id == "match_list":
            match_index = int(event.item.id.removeprefix("match-"))
            self._goto_match(match_index)
            return

        self._hide_search()

        filename = self.file_by_var_id[event.item.id]
        file_path = os.path.join(self.node_dir, filename)
        _, ext = os.path.splitext(filename)

        self.current_data = None
        self.current_text = ""
        self.view_kind = None

        if ext == ".csv":
            df = pd.read_csv(file_path, index_col=0)
            self.view_kind = "dataframe"
            self.current_data = df
            self.current_text = df.to_csv(index=False)
        elif ext == ".msgpack":
            with open(file_path, "rb") as f:
                data = msgpack.unpackb(f.read(), raw=False)
            self.view_kind = "msgpack"
            self.current_data = data
            self.current_text = json.dumps(data, indent=2, default=str)

        self._show_default_view()
