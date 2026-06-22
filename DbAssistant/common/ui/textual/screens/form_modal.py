"""Reusable modal form screen for the Textual UI.

Mirrors the Tk Toplevel "add connection" dialogs and the Web ``openFormModal``
helper so the three UIs collect the same fields in the same order. The screen
dismisses with a ``dict`` of field values on submit, or ``None`` on cancel.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, OptionList, Select, Static
from textual.widgets.option_list import Option


class FormModal(ModalScreen[dict | None]):
    """Collect a set of named fields and return them as a dict.

    ``fields`` is an iterable of dicts with keys:
        name, label, type ("text"|"password"|"select"|"checkbox"),
        value, options (for select: list of (value, label) or plain values).
    """

    DEFAULT_CSS = """
    FormModal {
        align: center middle;
    }
    FormModal > #form-box {
        width: 70;
        max-width: 90%;
        height: auto;
        max-height: 90%;
        padding: 1 2;
        border: round $accent;
        background: $surface;
    }
    FormModal #form-title {
        text-style: bold;
        padding-bottom: 1;
    }
    FormModal Input, FormModal Select {
        margin-bottom: 1;
    }
    FormModal #form-actions {
        height: auto;
        align: right middle;
    }
    FormModal #form-status {
        color: $error;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, title: str, fields: Iterable[dict],
                 submit_label: str = "Save", **kwargs) -> None:
        super().__init__(**kwargs)
        self._title = title
        self._fields = list(fields)
        self._submit_label = submit_label

    def compose(self) -> ComposeResult:
        with Vertical(id="form-box"):
            yield Static(self._title, id="form-title")
            for f in self._fields:
                fid = "field-" + f["name"]
                ftype = f.get("type", "text")
                if ftype == "select":
                    opts = []
                    for o in f.get("options", []):
                        if isinstance(o, (list, tuple)):
                            opts.append((str(o[1]), o[0]))
                        else:
                            opts.append((str(o), o))
                    yield Label(f.get("label", f["name"]))
                    yield Select(opts or [("(none)", "")], id=fid,
                                 value=f.get("value") or (opts[0][1] if opts else ""),
                                 allow_blank=False)
                elif ftype == "checkbox":
                    yield Checkbox(f.get("label", f["name"]), value=bool(f.get("value")), id=fid)
                else:
                    yield Label(f.get("label", f["name"]))
                    yield Input(value=str(f.get("value", "")), id=fid,
                                password=(ftype == "password"),
                                placeholder=f.get("placeholder", ""))
            yield Static("", id="form-status")
            with Vertical(id="form-actions"):
                yield Button(self._submit_label, id="form-submit", variant="primary")
                yield Button("Cancel", id="form-cancel")

    def _values(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in self._fields:
            w = self.query_one("#field-" + f["name"])
            if isinstance(w, Checkbox):
                out[f["name"]] = w.value
            elif isinstance(w, Select):
                out[f["name"]] = "" if w.value is Select.BLANK else w.value
            else:
                out[f["name"]] = w.value.strip()
        return out

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if (event.button.id or "") == "form-submit":
            self.dismiss(self._values())
        elif (event.button.id or "") == "form-cancel":
            self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)


class SelectModal(ModalScreen[Any]):
    """Pick one item from a list and return its value (or ``None`` on cancel).

    Mirrors the desktop "Load Saved" picker: instead of silently using the
    first/selected row, the user explicitly chooses from a scrollable list.

    ``options`` is a sequence of ``(label, value)`` pairs.
    """

    DEFAULT_CSS = """
    SelectModal {
        align: center middle;
    }
    SelectModal > #select-box {
        width: 80;
        max-width: 90%;
        height: auto;
        max-height: 90%;
        padding: 1 2;
        border: round $accent;
        background: $surface;
    }
    SelectModal #select-title {
        text-style: bold;
        padding-bottom: 1;
    }
    SelectModal OptionList {
        height: auto;
        max-height: 18;
        margin-bottom: 1;
    }
    SelectModal #select-actions {
        height: auto;
        align: right middle;
    }
    SelectModal #select-empty {
        color: $text-muted;
        padding-bottom: 1;
    }
    """

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, title: str, options: Sequence[tuple[str, Any]],
                 empty_message: str = "Nothing to choose from.", **kwargs) -> None:
        super().__init__(**kwargs)
        self._title = title
        self._options = list(options)
        self._empty_message = empty_message

    def compose(self) -> ComposeResult:
        with Vertical(id="select-box"):
            yield Static(self._title, id="select-title")
            if self._options:
                yield OptionList(
                    *[Option(label, id=str(idx))
                      for idx, (label, _value) in enumerate(self._options)],
                    id="select-list",
                )
            else:
                yield Static(self._empty_message, id="select-empty")
            with Vertical(id="select-actions"):
                yield Button("Cancel", id="select-cancel")

    def on_mount(self) -> None:
        if self._options:
            try:
                self.query_one("#select-list", OptionList).focus()
            except Exception:  # noqa: BLE001
                pass

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        try:
            idx = int(event.option.id)
        except (TypeError, ValueError):
            self.dismiss(None)
            return
        if 0 <= idx < len(self._options):
            self.dismiss(self._options[idx][1])
        else:
            self.dismiss(None)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if (event.button.id or "") == "select-cancel":
            self.dismiss(None)

    def action_cancel(self) -> None:
        self.dismiss(None)
