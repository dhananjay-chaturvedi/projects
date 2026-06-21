"""MetricsVisualizer graph store tests (deque logic without full Tk render)."""

from __future__ import annotations

from collections import deque

import pytest

from monitoring.metrics_visualizer import MetricGraph


class TestMetricGraphDeque:
    def test_max_points_trim(self):
        g = MetricGraph.__new__(MetricGraph)
        g.metric_name = "cpu"
        g.max_points = 3
        g.data = deque(maxlen=3)
        from datetime import datetime

        for i in range(5):
            g.data.append((datetime.now(), float(i)))
        assert len(g.data) == 3
        assert g.data[-1][1] == 4.0
