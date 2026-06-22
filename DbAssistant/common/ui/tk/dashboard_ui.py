"""
Tkinter dashboard tab — operational overview with module-aware panels.

Cards sit in a 2-column grid. Drag a card header onto any slot to swap/move.
Layout is persisted under ``<DBASSISTANT_HOME>/session/dashboard_layout.json``.
"""

from __future__ import annotations

import tkinter as tk
from dataclasses import dataclass, field
from tkinter import ttk
from typing import Any, Callable, Optional

from common.dashboard.layout_store import (
    load_layout,
    move_panel,
    reset_layout,
    save_layout,
)
from common.dashboard.service import (
    DashboardCapabilities,
    DashboardRuntime,
    DashboardService,
)
from common.ui.tk.theme import ColorTheme, default_ui_font
from common.ui.tk.widgets import make_scrollable


_STATUS_COLORS = {
    "healthy": ("#059669", "#ecfdf5"),
    "idle": ("#64748b", "#f8fafc"),
    "warning": ("#d97706", "#fffbeb"),
    "critical": ("#dc2626", "#fef2f2"),
    "degraded": ("#d97706", "#fffbeb"),
    "busy": ("#2563eb", "#eff6ff"),
    "running": ("#2563eb", "#eff6ff"),
    "ready": ("#2563eb", "#eff6ff"),
    "active": ("#059669", "#ecfdf5"),
    "alert": ("#dc2626", "#fef2f2"),
    "missing": ("#94a3b8", "#f1f5f9"),
    "monitoring": ("#059669", "#ecfdf5"),
}

_METRIC_TONE_FG = {
    "normal": None,
    "ok": "#059669",
    "warning": "#d97706",
    "critical": "#dc2626",
}

_METRIC_TONE_BG = {
    "normal": ColorTheme.BG_SECONDARY,
    "ok": ColorTheme.BG_SECONDARY,
    "warning": "#fffbeb",
    "critical": "#fef2f2",
}

_SEVERITY_COLORS = {
    "CRITICAL": ("#ffffff", "#dc2626"),
    "WARNING": ("#1e293b", "#fbbf24"),
    "INFO": ("#1e40af", "#dbeafe"),
}

_DROP_HIGHLIGHT = "#2563eb"
_CELL_PAD = 6


@dataclass
class _CardWidgets:
    frame: tk.Frame
    panel_id: str
    badge: tk.Label
    headline: tk.Label
    metrics_row: tk.Frame
    header: tk.Frame | None = None
    metric_values: list[tk.Label] = field(default_factory=list)
    metric_cells: list[tk.Frame] = field(default_factory=list)
    details_frame: tk.Frame | None = None
    detail_labels: list[tk.Label] = field(default_factory=list)
    nav_target: str | None = None
    on_card_click: Callable[[Any], None] | None = None


class DashboardUI:
    """Scrollable dashboard panel embedded in the master notebook."""

    def __init__(
        self,
        parent: tk.Widget,
        root: tk.Tk,
        *,
        runtime: DashboardRuntime,
        capabilities: DashboardCapabilities | None = None,
        on_navigate: Callable[[str], None] | None = None,
        status_callback: Callable[[str], None] | None = None,
        font_ui=None,
    ):
        self.parent = parent
        self.root = root
        self._on_navigate = on_navigate
        self._status = status_callback or (lambda _m: None)
        self._font_ui = font_ui or default_ui_font()
        self._title_font = (self._font_ui[0], self._font_ui[1] + 4, "bold")
        self._card_title_font = (self._font_ui[0], self._font_ui[1] + 2, "bold")
        self._service = DashboardService(runtime, capabilities)
        self._refresh_job = None
        self._tab_refresh_job = None
        self._content_frame: tk.Frame | None = None
        self._grid_container: tk.Frame | None = None
        self._canvas: tk.Canvas | None = None
        self._last_snapshot: dict[str, Any] | None = None
        self._layout: list[list[Optional[str]]] = load_layout()
        self._last_visible: set[str] = set()
        self._grid_built = False
        self._cells: dict[tuple[int, int], tk.Frame] = {}
        self._cards: dict[str, _CardWidgets] = {}
        self._hero_frame: tk.Frame | None = None
        self._hero_label: tk.Label | None = None
        self._alert_summary_frame: tk.Frame | None = None
        self._alerts_panel_frame: tk.Frame | None = None
        self._drag_panel_id: str | None = None
        self._drop_cell: tk.Frame | None = None
        self._card_wraplength = 440

    def create_ui(self) -> None:
        outer = ttk.Frame(self.parent)
        outer.pack(fill=tk.BOTH, expand=True)

        toolbar = ttk.Frame(outer)
        toolbar.pack(fill=tk.X, padx=12, pady=(8, 4))

        ttk.Label(toolbar, text="Dashboard", font=self._title_font).pack(side=tk.LEFT)
        self._updated_label = ttk.Label(
            toolbar, text="", foreground=ColorTheme.TEXT_SECONDARY
        )
        self._updated_label.pack(side=tk.LEFT, padx=(12, 0))
        ttk.Label(
            toolbar,
            text="Drag ⠿ header to rearrange",
            foreground=ColorTheme.TEXT_SECONDARY,
        ).pack(side=tk.LEFT, padx=(16, 0))

        ttk.Button(
            toolbar, text="Reset layout", command=self._reset_layout, width=12
        ).pack(side=tk.RIGHT, padx=4)
        ttk.Button(toolbar, text="Refresh", command=self.refresh_async, width=12).pack(
            side=tk.RIGHT, padx=4
        )

        self._content_frame = make_scrollable(outer, bg=ColorTheme.BG_MAIN)
        self._canvas = self._content_frame.scroll_canvas

        def _on_resize(event):
            half = max(280, (event.width - 64) // 2)
            if half != self._card_wraplength:
                self._card_wraplength = half
                self._apply_wraplengths()

        self._canvas.bind("<Configure>", _on_resize)

        self.root.bind("<ButtonRelease-1>", self._on_global_release, add="+")

    def on_tab_selected(self) -> None:
        """Refresh 1s after tab load, then resume periodic refresh."""
        if self._tab_refresh_job:
            self.root.after_cancel(self._tab_refresh_job)
            self._tab_refresh_job = None

        def _initial_refresh():
            self._tab_refresh_job = None
            self.refresh_async()
            self._schedule_auto_refresh()

        self._tab_refresh_job = self.root.after(1000, _initial_refresh)

    def on_tab_hidden(self) -> None:
        if self._tab_refresh_job:
            self.root.after_cancel(self._tab_refresh_job)
            self._tab_refresh_job = None
        if self._refresh_job:
            self.root.after_cancel(self._refresh_job)
            self._refresh_job = None
        self._clear_drag()

    def _schedule_auto_refresh(self) -> None:
        if self._refresh_job:
            self.root.after_cancel(self._refresh_job)

        def _tick():
            self.refresh_async()
            self._refresh_job = self.root.after(30000, _tick)

        self._refresh_job = self.root.after(30000, _tick)

    def refresh_async(self) -> None:
        """Refresh card data in place when possible (no full grid rebuild)."""
        self._status("Refreshing dashboard…")
        try:
            snap = self._service.collect()
        except Exception as exc:
            snap = {"error": str(exc), "timestamp": "—", "panels": [], "alerts": []}
        self._render(snap)

    def _reset_layout(self) -> None:
        visible = self._visible_panel_ids(
            (self._last_snapshot or {}).get("panels") or []
        )
        self._layout = reset_layout(visible)
        self._status("Dashboard layout reset")
        if self._last_snapshot:
            self._sync_cells_to_layout(self._last_snapshot.get("panels") or [])

    def _visible_panel_ids(self, panels: list[dict]) -> set[str]:
        return {p["id"] for p in panels if p.get("id")}

    def _render(self, snapshot: dict[str, Any]) -> None:
        if not self._content_frame:
            return

        self._last_snapshot = snapshot

        if snapshot.get("error"):
            self._destroy_chrome()
            tk.Label(
                self._content_frame,
                text=f"Dashboard error: {snapshot['error']}",
                fg=ColorTheme.ERROR,
                bg=ColorTheme.BG_MAIN,
                font=self._font_ui,
            ).pack(padx=20, pady=20)
            return

        self._updated_label.config(text=f"Updated {snapshot.get('timestamp', '')}")
        self._update_hero(snapshot)
        self._update_alert_summary(snapshot)
        self._update_alerts_panel(snapshot)

        panels = snapshot.get("panels") or []
        visible = self._visible_panel_ids(panels)
        self._layout = load_layout(visible)

        if not self._grid_built or visible != self._last_visible:
            self._build_grid(panels)
            self._last_visible = visible
        else:
            self._update_all_cards(panels)

        self._status("Dashboard ready")

    def _destroy_chrome(self) -> None:
        for child in self._content_frame.winfo_children():
            child.destroy()
        self._grid_built = False
        self._grid_container = None
        self._cells.clear()
        self._cards.clear()
        self._hero_frame = None
        self._hero_label = None
        self._alert_summary_frame = None
        self._alerts_panel_frame = None
        self._last_visible = set()

    def _build_grid(self, panels: list[dict]) -> None:
        if self._grid_container:
            self._grid_container.destroy()
        self._cells.clear()
        self._cards.clear()

        self._grid_container = tk.Frame(self._content_frame, bg=ColorTheme.BG_MAIN)
        self._grid_container.pack(fill=tk.X, padx=10, pady=(4, 12))

        for row_idx, row_ids in enumerate(self._layout):
            row_frame = tk.Frame(self._grid_container, bg=ColorTheme.BG_MAIN)
            row_frame.pack(fill=tk.X, pady=4)
            row_frame.columnconfigure(0, weight=1, uniform="dash_col")
            row_frame.columnconfigure(1, weight=1, uniform="dash_col")

            cols = list(row_ids[:2])
            while len(cols) < 2:
                cols.append(None)

            for col_idx, panel_id in enumerate(cols):
                cell = self._make_cell(row_frame, row_idx, col_idx)
                self._cells[(row_idx, col_idx)] = cell
                self._mount_cell(cell, panel_id, panels)

        self._grid_built = True

    def _make_cell(self, row_frame: tk.Frame, row_idx: int, col_idx: int) -> tk.Frame:
        cell = tk.Frame(
            row_frame,
            bg=ColorTheme.BG_MAIN,
            highlightbackground=ColorTheme.BORDER,
            highlightthickness=1,
        )
        pad_l = 0 if col_idx == 0 else _CELL_PAD
        pad_r = _CELL_PAD if col_idx == 0 else 0
        cell.grid(row=0, column=col_idx, sticky="nsew", padx=(pad_l, pad_r))
        cell._dash_row = row_idx  # type: ignore[attr-defined]
        cell._dash_col = col_idx  # type: ignore[attr-defined]
        cell._panel_id = None  # type: ignore[attr-defined]
        self._bind_drop_cell(cell)
        return cell

    def _clear_cell(self, cell: tk.Frame) -> None:
        for child in cell.winfo_children():
            child.destroy()
        cell._panel_id = None  # type: ignore[attr-defined]

    def _mount_cell(
        self, cell: tk.Frame, panel_id: Optional[str], panels: list[dict]
    ) -> None:
        self._clear_cell(cell)
        cell._panel_id = panel_id  # type: ignore[attr-defined]
        if not panel_id:
            self._render_empty_slot(cell)
            return
        by_id = {p["id"]: p for p in panels if p.get("id")}
        panel = by_id.get(panel_id)
        if panel:
            self._mount_panel_in_cell(cell, panel)
        else:
            self._render_empty_slot(cell)

    def _sync_cells_to_layout(self, panels: list[dict]) -> None:
        if not self._grid_built:
            self._build_grid(panels)
            return
        for (r, c), cell in self._cells.items():
            want = None
            if r < len(self._layout) and c < len(self._layout[r]):
                want = self._layout[r][c]
            if getattr(cell, "_panel_id", None) != want:
                self._mount_cell(cell, want, panels)

    def _update_all_cards(self, panels: list[dict]) -> None:
        by_id = {p["id"]: p for p in panels if p.get("id")}
        for panel_id, refs in list(self._cards.items()):
            panel = by_id.get(panel_id)
            if panel:
                self._update_panel_widgets(refs, panel)
            elif panel_id in self._cards:
                del self._cards[panel_id]

    def _apply_wraplengths(self) -> None:
        wrap = self._card_wraplength
        for refs in self._cards.values():
            refs.headline.config(wraplength=wrap)
            for lbl in refs.detail_labels:
                lbl.config(wraplength=wrap)

    def _rerender_grid(self) -> None:
        if self._last_snapshot:
            self._sync_cells_to_layout(self._last_snapshot.get("panels") or [])

    def _render_empty_slot(self, cell: tk.Frame) -> None:
        inner = tk.Frame(cell, bg=ColorTheme.BG_SECONDARY, height=120)
        inner.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        inner.pack_propagate(False)
        lbl = tk.Label(
            inner,
            text="Empty slot\n(drop a card here)",
            font=(self._font_ui[0], max(self._font_ui[1] - 1, 8)),
            fg=ColorTheme.TEXT_SECONDARY,
            bg=ColorTheme.BG_SECONDARY,
            justify=tk.CENTER,
        )
        lbl.pack(expand=True)
        self._bind_drop_hover(lbl, cell)

    def _bind_drop_cell(self, cell: tk.Frame) -> None:
        def _enter(_event=None, c=cell):
            if self._drag_panel_id:
                self._highlight_drop_cell(c)

        def _leave(_event=None, c=cell):
            if self._drop_cell is c and self._drag_panel_id:
                self._unhighlight_drop_cell(c)

        cell.bind("<Enter>", _enter)
        cell.bind("<Leave>", _leave)

    def _bind_drag_handle(self, widget: tk.Widget, panel_id: str, title: str) -> None:
        """Start a drag only from the card header (and its children)."""

        def _start(_event):
            self._drag_panel_id = panel_id
            self._status(f"Move «{title}» — release on any grid slot")

        def _bind_tree(w: tk.Widget) -> None:
            w.bind("<ButtonPress-1>", _start, add="+")
            try:
                w.configure(cursor="hand2")
            except tk.TclError:
                pass
            for child in w.winfo_children():
                _bind_tree(child)

        _bind_tree(widget)

    def _bind_drop_hover(self, widget: tk.Widget, cell: tk.Frame) -> None:
        """Highlight *cell* when dragging over card body (drop target only)."""

        def _enter(_event=None, c=cell):
            if self._drag_panel_id:
                self._highlight_drop_cell(c)

        def _bind_tree(w: tk.Widget) -> None:
            w.bind("<Enter>", _enter, add="+")
            for child in w.winfo_children():
                _bind_tree(child)

        _bind_tree(widget)

    def _highlight_drop_cell(self, cell: tk.Frame) -> None:
        if self._drop_cell and self._drop_cell is not cell:
            self._unhighlight_drop_cell(self._drop_cell)
        self._drop_cell = cell
        cell.configure(highlightbackground=_DROP_HIGHLIGHT, highlightthickness=2)

    def _unhighlight_drop_cell(self, cell: tk.Frame) -> None:
        cell.configure(highlightbackground=ColorTheme.BORDER, highlightthickness=1)
        if self._drop_cell is cell:
            self._drop_cell = None

    def _clear_drag(self) -> None:
        if self._drop_cell:
            self._unhighlight_drop_cell(self._drop_cell)
        self._drag_panel_id = None

    def _find_cell_at(self, x_root: int, y_root: int) -> tk.Frame | None:
        widget = self.root.winfo_containing(x_root, y_root)
        while widget is not None:
            if isinstance(widget, tk.Frame) and hasattr(widget, "_dash_row"):
                return widget
            widget = getattr(widget, "master", None)
        return None

    def _on_global_release(self, event=None) -> None:
        if not self._drag_panel_id or event is None:
            return
        cell = self._find_cell_at(event.x_root, event.y_root)
        if cell is not None:
            self._on_cell_drop(cell)
        else:
            self._clear_drag()

    def _on_cell_drop(self, cell: tk.Frame) -> None:
        if not self._drag_panel_id:
            return
        row = getattr(cell, "_dash_row", None)
        col = getattr(cell, "_dash_col", None)
        if row is None or col is None:
            self._clear_drag()
            return

        self._layout = move_panel(self._layout, self._drag_panel_id, row, col)
        save_layout(self._layout)
        self._clear_drag()
        self._status("Dashboard layout updated")
        if self._last_snapshot:
            self._sync_cells_to_layout(self._last_snapshot.get("panels") or [])

    def _make_card_click_handler(
        self, nav: str | None
    ) -> Callable[[Any], None] | None:
        """Closure that navigates to *nav* on a card body click.

        Returns ``None`` when navigation is unavailable so callers can skip the
        binding work entirely (keeps the cursor at its default ``arrow``).
        """
        if not self._on_navigate or not nav:
            return None

        def _click(_event=None, target=nav):
            # Don't navigate when the user is releasing a drag — that release
            # is handled by ``_on_global_release`` for layout reordering.
            if self._drag_panel_id:
                return
            try:
                self._on_navigate(target)
            except Exception as exc:
                self._status(f"Navigation failed: {exc}")

        return _click

    def _bind_card_body_click(
        self,
        card: tk.Widget,
        header: tk.Widget,
        details_frame: tk.Widget | None,
        on_click: Callable[[Any], None] | None,
    ) -> None:
        """Bind *on_click* to every widget inside *card* except the header.

        The header carries the drag handle (``⠿``) and keeps its existing
        ``<ButtonPress-1>`` binding for drag-and-drop. The ``details_frame``
        subtree is bound only at the frame level; its labels are already
        bound by ``_fill_detail_lines`` (which also re-binds them on refresh).
        """
        if on_click is None:
            return

        def _bind_one(widget: tk.Widget) -> None:
            try:
                widget.bind("<ButtonRelease-1>", on_click, add="+")
                widget.configure(cursor="hand2")
            except tk.TclError:
                pass

        def _walk(widget: tk.Widget) -> None:
            if widget is header:
                return
            _bind_one(widget)
            if widget is details_frame:
                return  # avoid double-binding the detail labels.
            for child in widget.winfo_children():
                _walk(child)

        _bind_one(card)
        for child in card.winfo_children():
            _walk(child)

    def _panel_style(self, panel: dict) -> tuple[str, str, str, str, int]:
        status = panel.get("status", "idle")
        if not panel.get("installed", True):
            status = "missing"
        accent, bg = _STATUS_COLORS.get(status, _STATUS_COLORS["idle"])
        border = (
            accent
            if status in ("critical", "degraded", "running", "alert", "monitoring")
            else ColorTheme.BORDER
        )
        thick = 2 if status in ("critical", "running", "degraded", "alert", "monitoring") else 1
        return status, accent, bg, border, thick

    def _apply_metric_tone(self, mcell: tk.Frame, val_lbl: tk.Label, tone: str) -> None:
        fg = _METRIC_TONE_FG.get(tone) or ColorTheme.TEXT_PRIMARY
        bg = _METRIC_TONE_BG.get(tone, ColorTheme.BG_SECONDARY)
        mcell.config(bg=bg)
        val_lbl.config(fg=fg, bg=bg)
        for child in mcell.winfo_children():
            if isinstance(child, tk.Label) and child is not val_lbl:
                child.config(bg=bg)

    def _mount_panel_in_cell(self, cell: tk.Frame, panel: dict) -> None:
        panel_id = panel.get("id", "")
        status, accent, bg, border, thick = self._panel_style(panel)
        wrap = self._card_wraplength
        title = panel.get("title", "")

        card = tk.Frame(
            cell,
            bg=bg,
            highlightbackground=border,
            highlightthickness=thick,
        )
        card.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        # Header row uses a consistent light-blue tint so it reads as a
        # distinct title bar regardless of the card's status colour.
        header_bg = ColorTheme.PRIMARY_LIGHT
        header = tk.Frame(card, bg=header_bg)
        header.pack(fill=tk.X, ipady=4)

        grip = tk.Label(
            header,
            text="⠿",
            font=(self._font_ui[0], self._font_ui[1] + 2),
            fg=ColorTheme.TEXT_SECONDARY,
            bg=header_bg,
        )
        grip.pack(side=tk.LEFT, padx=(12, 6))
        tk.Label(
            header,
            text=title,
            font=self._card_title_font,
            fg=ColorTheme.TEXT_PRIMARY,
            bg=header_bg,
            anchor=tk.W,
            justify=tk.LEFT,
        ).pack(side=tk.LEFT, fill=tk.X, expand=True)

        badge_text = status.replace("_", " ").title()
        if not panel.get("installed", True):
            badge_text = "Not installed"
        badge = tk.Label(
            header,
            text=badge_text,
            font=(self._font_ui[0], max(self._font_ui[1] - 1, 8), "bold"),
            fg=accent,
            bg=header_bg,
        )
        badge.pack(side=tk.RIGHT, padx=12)

        headline = tk.Label(
            card,
            text=panel.get("headline") or "",
            font=(self._font_ui[0], self._font_ui[1] + 1),
            fg=ColorTheme.TEXT_PRIMARY,
            bg=bg,
            wraplength=wrap,
            justify=tk.LEFT,
            anchor=tk.W,
        )
        headline.pack(fill=tk.X, padx=12, pady=(10, 8))

        metrics_row = tk.Frame(card, bg=bg)
        metrics_row.pack(fill=tk.X, padx=12, pady=(0, 6))
        metric_values: list[tk.Label] = []
        metric_cells: list[tk.Frame] = []
        for m in panel.get("metrics") or []:
            tone = m.get("tone", "normal")
            mcell = tk.Frame(
                metrics_row,
                bg=_METRIC_TONE_BG.get(tone, ColorTheme.BG_SECONDARY),
                highlightbackground=ColorTheme.BORDER,
                highlightthickness=1,
            )
            mcell.pack(side=tk.LEFT, padx=(0, 6), pady=2)
            lbl = tk.Label(
                mcell,
                text=str(m.get("label", "")),
                font=(self._font_ui[0], max(self._font_ui[1] - 1, 8)),
                fg=ColorTheme.TEXT_SECONDARY,
                bg=mcell.cget("bg"),
            )
            lbl.pack(anchor=tk.W, padx=8, pady=(6, 0))
            val_lbl = tk.Label(
                mcell,
                text=str(m.get("value", "")),
                font=(self._font_ui[0], self._font_ui[1], "bold"),
                fg=_METRIC_TONE_FG.get(tone) or ColorTheme.TEXT_PRIMARY,
                bg=mcell.cget("bg"),
            )
            val_lbl.pack(anchor=tk.W, padx=8, pady=(0, 8))
            metric_values.append(val_lbl)
            metric_cells.append(mcell)

        nav = panel.get("navigate") or None
        on_click = self._make_card_click_handler(nav)

        details_frame = tk.Frame(card, bg=bg)
        details_frame.pack(fill=tk.X, padx=12, pady=(0, 12))
        detail_labels = self._fill_detail_lines(
            details_frame, panel, bg, wrap, on_click=on_click
        )

        if panel_id:
            self._bind_drag_handle(header, panel_id, title)
            self._bind_drop_hover(card, cell)

        self._bind_card_body_click(card, header, details_frame, on_click)

        refs = _CardWidgets(
            frame=card,
            panel_id=panel_id,
            badge=badge,
            headline=headline,
            metrics_row=metrics_row,
            header=header,
            metric_values=metric_values,
            metric_cells=metric_cells,
            details_frame=details_frame,
            detail_labels=detail_labels,
            nav_target=nav,
            on_card_click=on_click,
        )
        if panel_id:
            self._cards[panel_id] = refs

    def _fill_detail_lines(
        self,
        parent: tk.Frame,
        panel: dict,
        bg: str,
        wrap: int,
        on_click: Callable[[Any], None] | None = None,
    ) -> list[tk.Label]:
        labels: list[tk.Label] = []
        for line in panel.get("detail_lines") or []:
            if not line:
                continue
            lbl = tk.Label(
                parent,
                text=f"• {line}",
                font=self._font_ui,
                fg=ColorTheme.TEXT_SECONDARY,
                bg=bg,
                wraplength=wrap,
                justify=tk.LEFT,
                anchor=tk.W,
            )
            lbl.pack(fill=tk.X, pady=1)
            if on_click is not None:
                lbl.bind("<ButtonRelease-1>", on_click, add="+")
                try:
                    lbl.configure(cursor="hand2")
                except tk.TclError:
                    pass
            labels.append(lbl)
        return labels

    def _update_panel_widgets(self, refs: _CardWidgets, panel: dict) -> None:
        status, accent, bg, border, thick = self._panel_style(panel)
        wrap = self._card_wraplength

        badge_text = status.replace("_", " ").title()
        if not panel.get("installed", True):
            badge_text = "Not installed"

        header_bg = ColorTheme.PRIMARY_LIGHT
        refs.frame.config(bg=bg, highlightbackground=border, highlightthickness=thick)
        refs.badge.config(text=badge_text, fg=accent, bg=header_bg)
        refs.headline.config(text=panel.get("headline") or "", bg=bg, wraplength=wrap)
        refs.metrics_row.config(bg=bg)
        if refs.header is not None:
            refs.header.config(bg=header_bg)
            for child in refs.header.winfo_children():
                if isinstance(child, tk.Label) and child is not refs.badge:
                    child.config(bg=header_bg)

        metrics = panel.get("metrics") or []
        for i, val_lbl in enumerate(refs.metric_values):
            if i < len(metrics):
                m = metrics[i]
                val_lbl.config(text=str(m.get("value", "")))
                if i < len(refs.metric_cells):
                    self._apply_metric_tone(
                        refs.metric_cells[i], val_lbl, m.get("tone", "normal")
                    )

        if refs.details_frame is not None:
            for lbl in refs.detail_labels:
                lbl.destroy()
            refs.detail_labels = self._fill_detail_lines(
                refs.details_frame, panel, bg, wrap, on_click=refs.on_card_click
            )

        self._recolor_skip_header = refs.header
        try:
            self._recolor_tree(refs.frame, bg)
        finally:
            self._recolor_skip_header = None

    def _recolor_tree(self, widget: tk.Widget, bg: str) -> None:
        # Subtree under the header keeps the title-bar tint untouched.
        header_widget = getattr(self, "_recolor_skip_header", None)
        if header_widget is not None and widget is header_widget:
            return
        try:
            if isinstance(widget, tk.Label) and widget.cget("bg") not in (
                ColorTheme.BG_SECONDARY,
                ColorTheme.PRIMARY_LIGHT,
                "#ffffff",
                "#dc2626",
                "#fbbf24",
                "#dbeafe",
            ):
                widget.config(bg=bg)
        except tk.TclError:
            pass
        for child in widget.winfo_children():
            if isinstance(child, ttk.Button):
                continue
            self._recolor_tree(child, bg)

    def _update_hero(self, snapshot: dict) -> None:
        overall = snapshot.get("overall_status", "idle")
        fg, bg = _STATUS_COLORS.get(overall, _STATUS_COLORS["idle"])
        text = snapshot.get("overall_label", "Overview")

        if self._hero_frame is None or self._hero_label is None:
            self._hero_frame = tk.Frame(self._content_frame, bg=bg)
            self._hero_frame.pack(fill=tk.X, padx=16, pady=(12, 8))
            self._hero_label = tk.Label(
                self._hero_frame,
                text=text,
                font=(self._font_ui[0], 18, "bold"),
                fg=fg,
                bg=bg,
            )
            self._hero_label.pack(anchor=tk.W, padx=20, pady=(16, 16))
        else:
            self._hero_frame.config(bg=bg)
            self._hero_label.config(text=text, fg=fg, bg=bg)

    def _update_alert_summary(self, snapshot: dict) -> None:
        summary = snapshot.get("alert_summary") or {}
        if self._alert_summary_frame:
            self._alert_summary_frame.destroy()
            self._alert_summary_frame = None

        if not summary.get("total"):
            return

        row = tk.Frame(self._content_frame, bg=ColorTheme.BG_MAIN)
        row.pack(fill=tk.X, padx=16, pady=(0, 8))
        self._alert_summary_frame = row

        for label, count, sev in (
            ("Critical", summary.get("CRITICAL", 0), "CRITICAL"),
            ("Warning", summary.get("WARNING", 0), "WARNING"),
            ("Info", summary.get("INFO", 0), "INFO"),
        ):
            if not count:
                continue
            fg, bg = _SEVERITY_COLORS.get(sev, _SEVERITY_COLORS["INFO"])
            chip = tk.Label(
                row,
                text=f"  {label}: {count}  ",
                font=(self._font_ui[0], self._font_ui[1], "bold"),
                fg=fg,
                bg=bg,
                padx=8,
                pady=6,
            )
            chip.pack(side=tk.LEFT, padx=(0, 8))

    def _update_alerts_panel(self, snapshot: dict) -> None:
        alerts = snapshot.get("alerts") or []
        if self._alerts_panel_frame:
            self._alerts_panel_frame.destroy()
            self._alerts_panel_frame = None

        if not alerts:
            return

        panel = tk.Frame(self._content_frame, bg=ColorTheme.BG_MAIN)
        panel.pack(fill=tk.X, padx=16, pady=(0, 10))
        self._alerts_panel_frame = panel

        tk.Label(
            panel,
            text="Recorded alerts (Monitor session & daemon)",
            font=self._card_title_font,
            fg=ColorTheme.TEXT_PRIMARY,
            bg=ColorTheme.BG_MAIN,
        ).pack(anchor=tk.W, pady=(0, 6))

        for alert in alerts[:15]:
            sev = str(alert.get("severity", "INFO")).upper()
            fg, bg = _SEVERITY_COLORS.get(sev, _SEVERITY_COLORS["INFO"])
            row = tk.Frame(panel, bg=bg)
            row.pack(fill=tk.X, pady=3)

            tk.Label(
                row,
                text=f" {sev} ",
                font=(self._font_ui[0], max(self._font_ui[1] - 1, 8), "bold"),
                fg=fg,
                bg=bg,
                width=10,
            ).pack(side=tk.LEFT, padx=(0, 8), pady=10)
            conn = alert.get("connection") or "system"
            tk.Label(
                row,
                text=f"[{conn}] {alert.get('message', '')}",
                font=self._font_ui,
                fg=ColorTheme.TEXT_PRIMARY,
                bg=bg,
                wraplength=880,
                justify=tk.LEFT,
                anchor=tk.W,
            ).pack(side=tk.LEFT, fill=tk.X, expand=True, pady=10)

        if self._on_navigate:
            ttk.Button(
                panel, text="Open Monitor tab", command=lambda: self._on_navigate("monitor")
            ).pack(anchor=tk.E, pady=(6, 0))
