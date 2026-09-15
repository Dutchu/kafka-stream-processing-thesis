"""Facade over the metrics_* modules (kept so `import metrics` keeps working).

Real code lives in metrics_e1.py, metrics_e4.py, metrics_e2.py, metrics_misc.py
(author's rule: no module above 600 lines).
"""
from __future__ import annotations

from metrics_e1 import *  # noqa: F401,F403
from metrics_e4 import *  # noqa: F401,F403
from metrics_e2 import *  # noqa: F401,F403
from metrics_misc import *  # noqa: F401,F403
from metrics_e1 import _col_mean, _col_median, _weighted_mean  # noqa: F401
from metrics_e4 import NIC_LIMIT_BPS, DISK_LIMIT_BPS, REQUEST_QUEUE_DURATION_THRESHOLD, _binding_resource  # noqa: F401
