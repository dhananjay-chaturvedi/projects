#-------------------------------------------------------------------------------
#description: Metric manager for the tool
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

"""
Metrics Visualizer - Lightweight graphing for monitoring metrics
Uses only tkinter Canvas - no external dependencies
"""

import tkinter as tk
from tkinter import ttk
from collections import deque
from datetime import datetime, timedelta
from config_loader import properties


class MetricGraph:
    """Simple line graph for a single metric"""

    def __init__(self, parent, metric_name, width=None, height=None, max_points=None):
        """
        Args:
            parent: Parent tkinter widget
            metric_name: Name of the metric
            width: Graph width in pixels (uses config default if None)
            height: Graph height in pixels (uses config default if None)
            max_points: Maximum data points (uses config default if None)
        """
        # Use configured defaults if not provided
        if width is None:
            width = properties.get_int('monitoring.graphs', 'metric_graph_width', default=200)
        if height is None:
            height = properties.get_int('monitoring.graphs', 'metric_graph_height', default=60)
        if max_points is None:
            max_points = properties.get_int('monitoring.limits', 'max_data_points', default=60)

        self.metric_name = metric_name
        self.width = width
        self.height = height
        self.max_points = max_points

        # Data storage: [(timestamp, value), ...]
        self.data = deque(maxlen=max_points)

        # Create frame
        self.frame = ttk.Frame(parent)

        # Create canvas (no fixed size - will expand to fill frame)
        self.canvas = tk.Canvas(
            self.frame,
            bg='white',
            highlightthickness=1,
            highlightbackground='#E0E0E0'
        )
        self.canvas.pack(fill=tk.BOTH, expand=True)

        # Bind click for magnification
        self.canvas.bind('<Button-1>', self.show_magnified)

        # Bind resize event to redraw graph
        self.canvas.bind('<Configure>', self._on_resize)

        self.magnified_window = None
        self._resize_timer = None

    def add_data_point(self, value, timestamp=None):
        """Add a new data point"""
        if timestamp is None:
            timestamp = datetime.now()

        # Convert to float if possible
        try:
            value = float(value)
        except (ValueError, TypeError):
            return

        self.data.append((timestamp, value))
        self.redraw()

    def _on_resize(self, event=None):
        """Handle canvas resize - debounced redraw"""
        if self._resize_timer:
            self.canvas.after_cancel(self._resize_timer)
        # Debounce: wait 100ms after last resize before redrawing
        self._resize_timer = self.canvas.after(100, self.redraw)

    def redraw(self):
        """Redraw the graph"""
        self.canvas.delete('all')

        # Get current canvas size (will change when window resizes)
        self.canvas.update_idletasks()
        canvas_width = self.canvas.winfo_width()
        canvas_height = self.canvas.winfo_height()

        # Use minimum size if canvas not yet rendered
        if canvas_width < 10:
            canvas_width = self.width
        if canvas_height < 10:
            canvas_height = self.height

        if len(self.data) < 2:
            # Not enough data to draw
            self.canvas.create_text(
                canvas_width // 2,
                canvas_height // 2,
                text="Collecting data...",
                fill='gray'
            )
            return

        # Get values
        values = [v for _, v in self.data]
        timestamps = [t for t, _ in self.data]

        # Calculate min/max for scaling
        min_val = min(values)
        max_val = max(values)

        # Add padding
        value_range = max_val - min_val
        if value_range == 0:
            value_range = 1

        padding = value_range * 0.1
        min_val -= padding
        max_val += padding

        # Draw grid lines (horizontal)
        for i in range(3):
            y = (canvas_height - 20) * i / 2 + 10
            self.canvas.create_line(
                10, y, canvas_width - 10, y,
                fill='#E0E0E0',
                dash=(2, 2)
            )

        # Draw the line graph
        points = []
        for i, (ts, val) in enumerate(self.data):
            x = 10 + (canvas_width - 20) * i / (len(self.data) - 1)
            y = canvas_height - 10 - (canvas_height - 20) * (val - min_val) / (max_val - min_val)
            points.append((x, y))

        # Draw line
        if len(points) > 1:
            for i in range(len(points) - 1):
                self.canvas.create_line(
                    points[i][0], points[i][1],
                    points[i + 1][0], points[i + 1][1],
                    fill='#2196F3',
                    width=2
                )

        # Draw points
        for x, y in points:
            self.canvas.create_oval(
                x - 2, y - 2, x + 2, y + 2,
                fill='#1976D2',
                outline='#1565C0'
            )

        # Draw min/max labels
        self.canvas.create_text(
            canvas_width - 30, canvas_height - 15,
            text=f"{min_val:.1f}",
            fill='gray',
            font=('Arial', 8)
        )
        self.canvas.create_text(
            canvas_width - 30, 15,
            text=f"{max_val:.1f}",
            fill='gray',
            font=('Arial', 8)
        )

        # Current value
        current_val = values[-1]
        self.canvas.create_text(
            10, 15,
            text=f"{current_val:.1f}",
            fill='#1976D2',
            font=('Arial', 10, 'bold'),
            anchor='w'
        )

    def show_magnified(self, event=None):
        """Show magnified view in a popup window"""
        if self.magnified_window and self.magnified_window.winfo_exists():
            self.magnified_window.lift()
            return

        self.magnified_window = tk.Toplevel()
        self.magnified_window.title(f"{self.metric_name} - Magnified View")
        self.magnified_window.geometry("800x600")

        # Create larger graph
        large_canvas = tk.Canvas(
            self.magnified_window,
            width=780,
            height=500,
            bg='white',
            highlightthickness=1,
            highlightbackground='#E0E0E0'
        )
        large_canvas.pack(padx=10, pady=10)

        if len(self.data) < 2:
            large_canvas.create_text(
                390, 250,
                text="Not enough data to display",
                fill='gray',
                font=('Arial', 14)
            )
            return

        # Draw magnified graph
        values = [v for _, v in self.data]
        timestamps = [t for t, _ in self.data]

        min_val = min(values)
        max_val = max(values)
        value_range = max_val - min_val
        if value_range == 0:
            value_range = 1

        padding = value_range * 0.1
        min_val -= padding
        max_val += padding

        # Draw grid
        for i in range(11):
            y = 50 + 400 * i / 10
            large_canvas.create_line(
                50, y, 730, y,
                fill='#E0E0E0' if i % 2 == 0 else '#F5F5F5',
                width=1
            )
            # Y-axis labels
            val = max_val - (max_val - min_val) * i / 10
            large_canvas.create_text(
                40, y,
                text=f"{val:.1f}",
                fill='gray',
                font=('Arial', 9),
                anchor='e'
            )

        # Draw vertical grid lines
        for i in range(13):
            x = 50 + 680 * i / 12
            large_canvas.create_line(
                x, 50, x, 450,
                fill='#F5F5F5',
                width=1
            )

        # Draw the line
        points = []
        for i, (ts, val) in enumerate(self.data):
            x = 50 + 680 * i / (len(self.data) - 1)
            y = 450 - 400 * (val - min_val) / (max_val - min_val)
            points.append((x, y))

        if len(points) > 1:
            for i in range(len(points) - 1):
                large_canvas.create_line(
                    points[i][0], points[i][1],
                    points[i + 1][0], points[i + 1][1],
                    fill='#2196F3',
                    width=3
                )

        # Draw points with hover info
        for i, (x, y) in enumerate(points):
            ts, val = self.data[i]
            large_canvas.create_oval(
                x - 3, y - 3, x + 3, y + 3,
                fill='#1976D2',
                outline='#1565C0',
                width=2,
                tags=f"point_{i}"
            )

            # Bind hover to show value
            large_canvas.tag_bind(
                f"point_{i}",
                '<Enter>',
                lambda e, v=val, t=ts: self._show_tooltip(large_canvas, e, v, t)
            )

        # Title
        large_canvas.create_text(
            390, 20,
            text=self.metric_name,
            fill='#1976D2',
            font=('Arial', 16, 'bold')
        )

        # Time range
        time_range = f"{timestamps[0].strftime('%H:%M')} - {timestamps[-1].strftime('%H:%M')}"
        large_canvas.create_text(
            390, 480,
            text=f"Time Range: {time_range} ({len(self.data)} data points)",
            fill='gray',
            font=('Arial', 10)
        )

        # Close button
        ttk.Button(
            self.magnified_window,
            text="Close",
            command=self.magnified_window.destroy
        ).pack(pady=5)

    def _show_tooltip(self, canvas, event, value, timestamp):
        """Show tooltip with value and timestamp"""
        # Remove old tooltip
        canvas.delete('tooltip')

        # Create tooltip
        text = f"{value:.2f}\n{timestamp.strftime('%H:%M:%S')}"

        # Background
        canvas.create_rectangle(
            event.x - 40, event.y - 30,
            event.x + 40, event.y - 5,
            fill='#FFFACD',
            outline='#FFA500',
            tags='tooltip'
        )

        # Text
        canvas.create_text(
            event.x, event.y - 17,
            text=text,
            fill='black',
            font=('Arial', 9),
            tags='tooltip'
        )

        # Auto-remove after 2 seconds
        canvas.after(2000, lambda: canvas.delete('tooltip'))

    def clear(self):
        """Clear all data"""
        self.data.clear()
        self.redraw()

    def pack(self, **kwargs):
        """Pack the frame"""
        self.frame.pack(**kwargs)

    def grid(self, **kwargs):
        """Grid the frame"""
        self.frame.grid(**kwargs)


class MetricsVisualizer:
    """Container for multiple metric graphs"""

    def __init__(self, parent, title="Metrics"):
        self.parent = parent
        self.title = title
        self.graphs = {}  # {metric_name: MetricGraph}
        self.separators_added = set()  # Track which separators have been added

        # Track rows for 2-column layout
        self.current_row_frame = None
        self.metrics_in_current_row = 0

        # Create scrollable container with both vertical and horizontal scrolling
        self.canvas = tk.Canvas(parent, highlightthickness=0)
        self.v_scrollbar = ttk.Scrollbar(parent, orient=tk.VERTICAL, command=self.canvas.yview)
        self.h_scrollbar = ttk.Scrollbar(parent, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.canvas_window = self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")

        def _on_frame_configure(event=None):
            """Update scroll region when content changes"""
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))

        def _on_canvas_configure(event):
            """Set minimum width for scrollable_frame, but allow it to grow wider"""
            canvas_width = event.width
            # Get the actual required width of the frame content
            self.scrollable_frame.update_idletasks()
            required_width = self.scrollable_frame.winfo_reqwidth()

            # Only set width if canvas is wider than required (prevent shrinking below content width)
            if canvas_width > required_width:
                self.canvas.itemconfig(self.canvas_window, width=canvas_width)
            else:
                # Let frame use its natural width (enables horizontal scrolling)
                self.canvas.itemconfig(self.canvas_window, width=required_width)

        self.scrollable_frame.bind("<Configure>", _on_frame_configure)
        self.canvas.bind("<Configure>", _on_canvas_configure)

        self.canvas.configure(
            yscrollcommand=self.v_scrollbar.set,
            xscrollcommand=self.h_scrollbar.set
        )

        # Don't pack by default - let the parent control visibility
        # self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # self.v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        # self.h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

    def add_metric(self, metric_name, width=200, height=60, max_points=60):
        """Add a new metric graph (2 per row)"""
        if metric_name not in self.graphs:
            # Create new row if needed (every 2 metrics)
            if self.current_row_frame is None or self.metrics_in_current_row >= 2:
                self.current_row_frame = ttk.Frame(self.scrollable_frame)
                self.current_row_frame.pack(fill=tk.X, padx=5, pady=5)
                self.metrics_in_current_row = 0

            # Create metric container within the row
            metric_container = ttk.Frame(self.current_row_frame)
            metric_container.pack(side=tk.LEFT, padx=10, pady=5, fill=tk.BOTH, expand=True)

            # Header frame - contains metric name and value horizontally
            header_frame = ttk.Frame(metric_container)
            header_frame.pack(side=tk.TOP, fill=tk.X, pady=(0, 3))

            # Label - metric name
            label = ttk.Label(
                header_frame,
                text=f"{metric_name}:",
                font=('Arial', 10),
                anchor='w'
            )
            label.pack(side=tk.LEFT, padx=(0, 5))

            # Value label
            value_label = ttk.Label(
                header_frame,
                text="--",
                font=('Arial', 10, 'bold'),
                anchor='w'
            )
            value_label.pack(side=tk.LEFT)

            # Create the graph below the header
            graph = MetricGraph(metric_container, metric_name, width, height, max_points)
            self.graphs[metric_name] = graph

            # Pack the graph frame below the header
            graph.frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

            # Store references
            graph.value_label = value_label
            graph.metric_container = metric_container

            # Increment counter
            self.metrics_in_current_row += 1

            # Update scrollregion after adding metric
            self.scrollable_frame.update_idletasks()
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))

        return self.graphs[metric_name]

    def update_metric(self, metric_name, value):
        """Update a metric with new value"""
        if metric_name in self.graphs:
            graph = self.graphs[metric_name]
            graph.add_data_point(value)

            # Update value label
            if hasattr(graph, 'value_label'):
                try:
                    if isinstance(value, float):
                        graph.value_label.config(text=f"{value:.2f}")
                    else:
                        graph.value_label.config(text=str(value))
                except (ValueError, TypeError, AttributeError):
                    graph.value_label.config(text="--")

    def add_separator(self, label=None):
        """Add a visual separator between sections"""
        # Check if separator with this label already exists
        if label and label in self.separators_added:
            return  # Don't add duplicate separators

        # Reset row tracking for new section
        self.current_row_frame = None
        self.metrics_in_current_row = 0

        # Add label first (above separator) if provided
        if label:
            label_widget = ttk.Label(
                self.scrollable_frame,
                text=label,
                font=('Arial', 11, 'bold'),
                foreground='#1976D2'
            )
            label_widget.pack(anchor=tk.W, padx=10, pady=(15, 2))
            # Track that this separator has been added
            self.separators_added.add(label)

        # Add separator line below the label
        separator_frame = ttk.Frame(self.scrollable_frame, height=2, relief=tk.GROOVE, borderwidth=1)
        separator_frame.pack(fill=tk.X, padx=10, pady=(0, 5))

        # Update scrollregion after adding separator
        self.scrollable_frame.update_idletasks()
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def clear_all(self):
        """Clear all graphs"""
        for graph in self.graphs.values():
            graph.clear()
        # Clear separator tracking
        self.separators_added.clear()
