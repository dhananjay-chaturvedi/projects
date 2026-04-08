#-------------------------------------------------------------------------------
#description: Widgets manager for the tool
#initial version: 08-APR-2026
#Author: Dhananjay Chaturvedi
#Copyright 2026 Dhananjay Chaturvedi
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#-------------------------------------------------------------------------------

"""UI Widget Utilities - Reusable widget helper functions"""
import sys
import tkinter as tk
from tkinter import ttk

from .theme import ColorTheme


def bind_canvas_mousewheel(canvas):
    """Scroll canvas with wheel while pointer is over it (macOS / Windows / Linux)."""

    if sys.platform == "linux":
        canvas.bind("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))
        return

    def _wheel_mousewheel(event):
        if sys.platform == "darwin":
            canvas.yview_scroll(int(-1 * event.delta), "units")
        else:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _enter(_event):
        canvas.bind_all("<MouseWheel>", _wheel_mousewheel)

    # Don't unbind on leave - just let the next canvas's Enter rebind
    # This prevents the "dead zone" issue when leaving a nested canvas
    canvas.bind("<Enter>", _enter)

    # Bind immediately so scrolling works right away
    # When mouse enters a different canvas, that canvas's Enter event will rebind
    canvas.bind_all("<MouseWheel>", _wheel_mousewheel)


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

    canvas_window = canvas.create_window((0, 0), window=scrollable_frame, anchor=tk.NW)
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
    icon_label = tk.Label(header,
                         text="▾" if expanded else "▸",
                         font=("Arial", 12),
                         foreground="#0ea5e9",
                         bg=ColorTheme.BG_MAIN,
                         width=2,
                         cursor="hand2")
    icon_label.pack(side=tk.LEFT, padx=(0, 6), pady=8)

    # Simple title
    title_label = tk.Label(header,
                          text=title,
                          font=title_font,
                          foreground="#475569",
                          bg=ColorTheme.BG_MAIN,
                          anchor=tk.W,
                          cursor="hand2")
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
