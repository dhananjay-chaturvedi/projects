# ---------------------------------------------------------------------
# description: Widgets manager for the tool
# initial version: 08-APR-2026
# Author: Dhananjay Chaturvedi
# ---------------------------------------------------------------------

"""UI Widget Utilities - Reusable widget helper functions"""

import sys
import tkinter as tk
from tkinter import ttk

from .theme import ColorTheme


# Track whether the global wheel router has already been installed on a Tk
# interpreter. ``bind_all`` is per-interpreter, so one install is enough even
# when many canvases register themselves.
_WHEEL_ROUTER_INSTALLED: "set[str]" = set()

# Native widgets that implement their own wheel scrolling. When the pointer is
# over one of these the router defers to it *only if* the widget can actually
# scroll (see ``_can_scroll_vertically``); otherwise the wheel falls through to
# the enclosing scrollable frame. ``ttk.Combobox`` is handled separately because
# a closed combobox must never consume the surrounding frame's wheel.
_NATIVE_WHEEL_CLASSES = (
    "Listbox",
    "Text",
    "Spinbox",
    "Treeview",
)


def _scroll_units_from_delta(raw: float) -> int:
    """Convert a raw platform wheel delta into ``yview_scroll`` units.

    Returns sign-correct units (negative = up) and guarantees at least one unit
    for any non-zero motion. This matters on Tk 9 where high-resolution
    trackpads report small/fractional deltas: a plain ``int()`` truncation would
    drop them to zero and the view would never move.
    """
    if not raw:
        return 0
    if sys.platform == "darwin":
        # macOS reports already-small deltas (mouse: ±1..±3; trackpad: fractional).
        # Positive delta = scroll up, so invert the sign.
        steps = int(raw)
        if steps == 0:
            steps = 1 if raw > 0 else -1
        return -steps
    # Windows: classic mice report multiples of 120; hi-res devices report less.
    if abs(raw) >= 120:
        return int(-raw / 120)
    return -1 if raw > 0 else 1


def _can_scroll_vertically(widget) -> bool:
    """True if *widget* has a vertical view whose content actually overflows."""
    try:
        first, last = widget.yview()
    except Exception:
        return False
    return first > 1e-6 or last < 1.0 - 1e-6


def _can_scroll_horizontally(widget) -> bool:
    """True if *widget* has a horizontal view whose content actually overflows."""
    try:
        first, last = widget.xview()
    except Exception:
        return False
    return first > 1e-6 or last < 1.0 - 1e-6


# Touchpad deltas are small per-event pixel values; accumulate them so a slow
# drag still adds up to whole scroll "units" (canvas only supports unit/page
# scrolling, not pixels). ~5px per unit roughly matches Tk's own Listbox feel.
_TOUCH_PX_PER_UNIT = 5.0
_touch_accum = 0.0
_touch_x_accum = 0.0


def _route_wheel_core(units: int, base_widget, *, axis: str = "y"):
    """Scroll the first genuinely-scrollable ancestor under the pointer.

    Resolves the widget under the *current pointer* and walks up from it so the
    wheel always affects the region the pointer is in:
      * A native scrollable widget (Text / Listbox / Treeview / Spinbox) keeps
        the wheel only when its own content overflows; otherwise we fall through
        so the enclosing frame scrolls instead.
      * A closed ``ttk.Combobox`` never consumes the wheel (its value-cycling
        wheel is disabled elsewhere), so we always fall through past it.
      * A canvas marked scrollable scrolls when its content overflows; if the
        content fits we fall through to an outer scrollable canvas.

    Returns ``"break"`` when it scrolls (to stop further bindings), else None.
    """
    if not units:
        return None
    try:
        x_root, y_root = base_widget.winfo_pointerxy()
        widget = base_widget.winfo_containing(x_root, y_root)
    except Exception:
        return None

    while widget is not None:
        cls = widget.winfo_class()

        if cls == "TCombobox":
            # Closed combobox: don't let it eat the surrounding frame's wheel.
            widget = widget.master
            continue

        if cls in _NATIVE_WHEEL_CLASSES:
            # Defer to the widget's own handler only when it can really scroll;
            # otherwise fall through to scroll the enclosing frame.
            if axis == "x" and _can_scroll_horizontally(widget):
                return None
            if axis != "x" and _can_scroll_vertically(widget):
                return None
            widget = widget.master
            continue

        if isinstance(widget, tk.Canvas) and getattr(
            widget, "_wheel_scrollable", False
        ):
            if axis == "x" and _can_scroll_horizontally(widget):
                widget.xview_scroll(units, "units")
                return "break"
            if axis != "x" and _can_scroll_vertically(widget):
                widget.yview_scroll(units, "units")
                return "break"
            # Content fits entirely - let an outer canvas handle the wheel.

        widget = widget.master

    return None


def _route_wheel_x11(event):
    """X11 adapter: Button-4/5 carry direction in ``event.num``."""
    units = -1 if getattr(event, "num", 0) == 4 else 1
    return _route_wheel_core(units, event.widget)


def _route_shift_wheel_x11(event):
    """X11 adapter: Shift+Button-4/5 scroll horizontally."""
    units = -1 if getattr(event, "num", 0) == 4 else 1
    return _route_wheel_core(units, event.widget, axis="x")


def _make_raw_wheel_handler(base_widget, *, axis: str = "y"):
    """Build a Tcl-level ``<MouseWheel>`` handler that reads the *raw* delta.

    tkinter parses ``%D`` with ``getint`` and zeroes it when Tk 9 reports a
    fractional delta (e.g. ``"0.3"``). Reading ``%D`` ourselves as a float
    preserves those events. Note: on Tk 9 the two-finger trackpad gesture is
    delivered as ``<TouchpadScroll>`` (handled separately), so this path is
    mainly the physical mouse wheel.
    """

    def handler(d):
        try:
            raw = float(d)
        except (TypeError, ValueError):
            return ""
        units = _scroll_units_from_delta(raw)
        return _route_wheel_core(units, base_widget, axis=axis) or ""

    return handler


def _make_raw_touchpad_handler(base_widget, *, axis: str = "y"):
    """Build a Tcl-level ``<TouchpadScroll>`` handler (Tk 9 high-res trackpad).

    ``%D`` packs signed 16-bit ΔX/ΔY (ΔX high word, ΔY low word), exactly as
    ``tk::PreciseScrollDeltas`` unpacks them. We use the vertical delta and
    accumulate small pixel deltas into whole scroll units.
    """

    def handler(d):
        global _touch_accum, _touch_x_accum
        try:
            packed = int(d)
        except (TypeError, ValueError):
            return ""
        high = (packed >> 16) & 0xFFFF
        delta_x = high if high < 0x8000 else high - 0x10000
        low = packed & 0xFFFF
        delta_y = low if low < 0x8000 else low - 0x10000
        delta = delta_x if axis == "x" else delta_y
        if delta == 0:
            return ""
        # Native widgets scroll by negative deltas; accumulate so slow drags count.
        if axis == "x":
            _touch_x_accum += -delta
            units = int(_touch_x_accum / _TOUCH_PX_PER_UNIT)
        else:
            _touch_accum += -delta
            units = int(_touch_accum / _TOUCH_PX_PER_UNIT)
        if units == 0:
            return ""
        if axis == "x":
            _touch_x_accum -= units * _TOUCH_PX_PER_UNIT
        else:
            _touch_accum -= units * _TOUCH_PX_PER_UNIT
        return _route_wheel_core(units, base_widget, axis=axis) or ""

    return handler


def _make_combo_wheel_handler(base_widget):
    """``<MouseWheel>`` handler for the ``TCombobox`` class tag.

    Routes the wheel to the enclosing scrollable frame (so the form still
    scrolls) and always returns "break" so the combobox never cycles its value
    and the shared 'all' router doesn't double-handle the same event.
    """
    raw = _make_raw_wheel_handler(base_widget)

    def handler(d):
        raw(d)
        return "break"

    return handler


def _make_combo_touchpad_handler(base_widget):
    """``<TouchpadScroll>`` analogue of :func:`_make_combo_wheel_handler`."""
    raw = _make_raw_touchpad_handler(base_widget)

    def handler(d):
        raw(d)
        return "break"

    return handler


def _tcl_bind(toplevel, tag, sequence, handler, *, add):
    """Bind *sequence* on *tag* at the Tcl level, reading the raw ``%D``.

    ``add=True`` appends (for the shared 'all' tag); ``add=False`` replaces the
    tag's existing binding (used to override ttk's value-cycling combobox
    bindings). When *handler* returns "break", Tcl's ``break`` stops the
    remaining bindings — replicating tkinter's own bind wrapper. Raises
    ``tk.TclError`` for unknown event sequences (e.g. ``<TouchpadScroll>`` on
    Tk 8.6).
    """
    funcid = toplevel.register(handler)
    prefix = "+" if add else ""
    toplevel.tk.call(
        "bind", tag, sequence, prefix + 'if {"[' + funcid + ' %D]" == "break"} break'
    )


def _install_wheel_router(canvas: tk.Canvas) -> None:
    try:
        toplevel = canvas.winfo_toplevel()
        key = str(toplevel)
    except Exception:
        return
    if key in _WHEEL_ROUTER_INSTALLED:
        return
    _WHEEL_ROUTER_INSTALLED.add(key)
    if sys.platform == "linux":
        toplevel.bind_all("<Button-4>", _route_wheel_x11, add="+")
        toplevel.bind_all("<Button-5>", _route_wheel_x11, add="+")
        toplevel.bind_all("<Shift-Button-4>", _route_shift_wheel_x11, add="+")
        toplevel.bind_all("<Shift-Button-5>", _route_shift_wheel_x11, add="+")
        return

    # macOS / Windows. Bind at the Tcl level so we receive the raw delta via
    # %D, because tkinter's bind would zero fractional deltas (Tk 9).
    _tcl_bind(toplevel, "all", "<MouseWheel>", _make_raw_wheel_handler(toplevel), add=True)
    _tcl_bind(
        toplevel, "all", "<Shift-MouseWheel>",
        _make_raw_wheel_handler(toplevel, axis="x"), add=True,
    )
    # Tk 9 high-resolution trackpad: the two-finger gesture is delivered as
    # <TouchpadScroll>, NOT <MouseWheel>, so it needs its own binding or the
    # wheel never fires over plain content frames.
    try:
        _tcl_bind(
            toplevel,
            "all",
            "<TouchpadScroll>",
            _make_raw_touchpad_handler(toplevel),
            add=True,
        )
        _tcl_bind(
            toplevel,
            "all",
            "<Shift-TouchpadScroll>",
            _make_raw_touchpad_handler(toplevel, axis="x"),
            add=True,
        )
    except tk.TclError:
        # Older Tk (8.6) has no TouchpadScroll event; trackpads fall back to
        # <MouseWheel> there, which is already handled above.
        pass


def bind_canvas_mousewheel(canvas):
    """Make ``canvas`` participate in pointer-aware wheel scrolling.

    Wheel events are routed by a single root-level handler that hit-tests the
    cursor position, so the inner canvas only scrolls while the pointer is
    truly over it and the outer canvas takes over the moment the pointer
    leaves the inner one.
    """
    canvas._wheel_scrollable = True
    _install_wheel_router(canvas)


def make_scrollable(parent, *, bg=None, fit_width=True):
    """Create an auto-hiding, both-axis scrollable frame.

    The returned ``inner`` frame is where callers build their UI. The outer
    container is packed into *parent* with ``fill=BOTH, expand=True``.
    """
    if bg is None:
        bg = ColorTheme.BG_MAIN

    shell = ttk.Frame(parent)
    shell.pack(fill=tk.BOTH, expand=True)
    shell.rowconfigure(0, weight=1)
    shell.columnconfigure(0, weight=1)

    canvas = tk.Canvas(shell, highlightthickness=0, bd=0, bg=bg)
    v_scroll = ttk.Scrollbar(shell, orient=tk.VERTICAL, command=canvas.yview)
    h_scroll = ttk.Scrollbar(shell, orient=tk.HORIZONTAL, command=canvas.xview)
    inner = ttk.Frame(canvas)
    window_id = canvas.create_window((0, 0), window=inner, anchor=tk.NW)

    canvas.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
    canvas.grid(row=0, column=0, sticky="nsew")

    state = {
        "sync_after": None,
        "v_visible": False,
        "h_visible": False,
    }

    def _show(widget, key, grid_kwargs):
        if not state[key]:
            widget.grid(**grid_kwargs)
            state[key] = True

    def _hide(widget, key):
        if state[key]:
            widget.grid_remove()
            state[key] = False

    def _sync():
        state["sync_after"] = None
        bbox = canvas.bbox("all")
        if not bbox:
            canvas.configure(scrollregion=(0, 0, 0, 0))
            _hide(v_scroll, "v_visible")
            _hide(h_scroll, "h_visible")
            return

        req_w = max(inner.winfo_reqwidth(), bbox[2] - bbox[0])
        req_h = max(inner.winfo_reqheight(), bbox[3] - bbox[1])
        canvas_w = max(canvas.winfo_width(), 1)
        canvas_h = max(canvas.winfo_height(), 1)

        needs_h = req_w > canvas_w + 1
        if fit_width and not needs_h:
            canvas.itemconfigure(window_id, width=canvas_w)
        else:
            canvas.itemconfigure(window_id, width=0)

        # Re-read after width changes because wrapping labels can affect height.
        inner.update_idletasks()
        bbox = canvas.bbox("all") or bbox
        req_w = max(inner.winfo_reqwidth(), bbox[2] - bbox[0])
        req_h = max(inner.winfo_reqheight(), bbox[3] - bbox[1])
        needs_h = req_w > canvas_w + 1
        needs_v = req_h > canvas_h + 1

        canvas.configure(scrollregion=bbox)
        if needs_v:
            _show(v_scroll, "v_visible", {"row": 0, "column": 1, "sticky": "ns"})
        else:
            _hide(v_scroll, "v_visible")
        if needs_h:
            _show(h_scroll, "h_visible", {"row": 1, "column": 0, "sticky": "ew"})
        else:
            _hide(h_scroll, "h_visible")

    def _schedule_sync(_event=None):
        try:
            if state["sync_after"] is not None:
                shell.after_cancel(state["sync_after"])
            state["sync_after"] = shell.after(80, _sync)
        except tk.TclError:
            pass

    inner.bind("<Configure>", _schedule_sync)
    canvas.bind("<Configure>", _schedule_sync)
    bind_canvas_mousewheel(canvas)
    _schedule_sync()

    # Expose internals for the few call sites that need direct canvas access.
    inner.scroll_canvas = canvas
    inner.scroll_container = shell
    inner.v_scrollbar = v_scroll
    inner.h_scrollbar = h_scroll
    inner.sync_scrollregion = _schedule_sync
    return inner


# Tk-class-level override flag — keyed by the same toplevel as the router so
# repeat init across windows is idempotent without leaking state.
_COMBOBOX_WHEEL_DISABLED: "set[str]" = set()


def disable_combobox_mousewheel(root: tk.Misc) -> None:
    """Stop scroll gestures from cycling values on closed ``ttk.Combobox``es.

    The Tk class bindings for ``<MouseWheel>`` / ``<TouchpadScroll>`` on
    ``TCombobox`` (and ``Button-4`` / ``Button-5`` on X11) cycle to the
    previous/next list item, which makes it dangerously easy to silently change
    a saved setting while scrolling a surrounding form. We *replace* those class
    bindings so that, instead of cycling the value, the gesture is routed to the
    enclosing scrollable frame — the surrounding form scrolls and the combobox
    value is left untouched. Users still have to *click* the dropdown to change
    it. ``<TouchpadScroll>`` matters on Tk 9: the two-finger trackpad gesture is
    delivered as that event, not ``<MouseWheel>``.

    The popup that appears when the combobox is opened is a separate ``Listbox``
    widget (different class), so its wheel scrolling — and every other
    ``Listbox`` / ``Text`` / ``Treeview`` in the app — is unaffected.
    """
    try:
        toplevel = root.winfo_toplevel()
        key = str(toplevel)
    except Exception:
        return
    if key in _COMBOBOX_WHEEL_DISABLED:
        return
    _COMBOBOX_WHEEL_DISABLED.add(key)

    if sys.platform == "linux":
        def _combo_x11(event):
            units = -1 if getattr(event, "num", 0) == 4 else 1
            _route_wheel_core(units, event.widget)
            return "break"  # never cycle the value

        for seq in ("<Button-4>", "<Button-5>", "<MouseWheel>"):
            try:
                root.bind_class("TCombobox", seq, _combo_x11)
            except tk.TclError:
                pass
        return

    # macOS / Windows: replace the class bindings (add=False) so ttk's
    # value-cycling handlers are removed and our routing handlers take over.
    _tcl_bind(
        toplevel, "TCombobox", "<MouseWheel>",
        _make_combo_wheel_handler(toplevel), add=False,
    )
    try:
        _tcl_bind(
            toplevel, "TCombobox", "<TouchpadScroll>",
            _make_combo_touchpad_handler(toplevel), add=False,
        )
    except tk.TclError:
        pass


def create_horizontal_scrollable(parent, bg=None):
    """
    Create a horizontally scrollable frame with auto-sizing canvas.
    Returns the scrollable frame where you can pack widgets.

    This is optimized to:
    - Auto-calculate height based on content
    - Minimize memory usage
    - Provide smooth horizontal scrolling
    """
    if bg is None:
        bg = ColorTheme.BG_MAIN

    # Canvas without fixed height - will auto-size
    canvas = tk.Canvas(parent, highlightthickness=0, bd=0, bg=bg)
    scrollbar = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=canvas.xview)
    scrollable_frame = ttk.Frame(canvas)

    canvas.create_window((0, 0), window=scrollable_frame, anchor=tk.NW)
    canvas.configure(xscrollcommand=scrollbar.set)

    def on_frame_configure(event):
        # Update scroll region
        canvas.configure(scrollregion=canvas.bbox("all"))
        # Auto-adjust canvas height to fit content
        req_height = scrollable_frame.winfo_reqheight()
        if req_height > 0:
            canvas.configure(height=req_height)

    scrollable_frame.bind("<Configure>", on_frame_configure)

    canvas.pack(fill=tk.X, expand=False)  # Only expand horizontally
    scrollbar.pack(fill=tk.X)

    return scrollable_frame


def make_collapsible_section(parent, title, title_font, expanded=True):
    """Minimal collapsible section - lightweight and clean."""
    # Simple flat container
    shell = tk.Frame(parent, bg=ColorTheme.BG_MAIN, relief=tk.FLAT, bd=0)
    shell.pack(fill=tk.X, pady=3, padx=0)

    # Clean header
    header = tk.Frame(shell, bg=ColorTheme.BG_MAIN, cursor="hand2")
    header.pack(fill=tk.X)

    state = {"collapsed": not expanded}

    # Minimal icon
    icon_label = tk.Label(
        header,
        text="▾" if expanded else "▸",
        font=("Arial", 12),
        foreground="#0ea5e9",
        bg=ColorTheme.BG_MAIN,
        width=2,
        cursor="hand2",
    )
    icon_label.pack(side=tk.LEFT, padx=(0, 6), pady=8)

    # Simple title
    title_label = tk.Label(
        header,
        text=title,
        font=title_font,
        foreground="#475569",
        bg=ColorTheme.BG_MAIN,
        anchor=tk.W,
        cursor="hand2",
    )
    title_label.pack(side=tk.LEFT, fill=tk.X, expand=True, pady=8)

    # Content frame
    content_wrapper = tk.Frame(shell, bg=ColorTheme.BG_MAIN)
    content = tk.Frame(content_wrapper, bg=ColorTheme.BG_MAIN)

    def set_collapsed(collapsed):
        state["collapsed"] = collapsed
        if collapsed:
            icon_label.config(text="▸")
            content_wrapper.pack_forget()
        else:
            icon_label.config(text="▾")
            content_wrapper.pack(fill=tk.X)
            content.pack(fill=tk.X, padx=25, pady=(0, 8))

    def toggle_section(event=None):
        set_collapsed(not state["collapsed"])

    header.bind("<Button-1>", toggle_section)
    icon_label.bind("<Button-1>", toggle_section)
    title_label.bind("<Button-1>", toggle_section)

    # Subtle hover effect
    def on_enter(e):
        title_label.config(foreground="#0ea5e9")

    def on_leave(e):
        title_label.config(foreground="#475569")

    header.bind("<Enter>", on_enter)
    header.bind("<Leave>", on_leave)

    if expanded:
        content_wrapper.pack(fill=tk.X)
        content.pack(fill=tk.X, padx=25, pady=(0, 8))

    return content
