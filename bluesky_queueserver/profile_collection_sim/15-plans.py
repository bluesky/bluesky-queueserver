# flake8: noqa
from typing import List, Optional, Dict, Any

from bluesky.plans import (
    count,
    list_scan,
    rel_list_scan,
    list_grid_scan,
    rel_list_grid_scan,
    log_scan,
    rel_log_scan,
    adaptive_scan,
    rel_adaptive_scan,
    tune_centroid,
    scan_nd,
    inner_product_scan,
    scan,
    grid_scan,
    rel_grid_scan,
    relative_inner_product_scan,
    rel_scan,
    tweak,
    spiral_fermat,
    rel_spiral_fermat,
    spiral,
    rel_spiral,
    spiral_square,
    rel_spiral_square,
    ramp_plan,
    fly,
    x2x_scan,
)


def marked_up_count(
    detectors: List[Any], num: int = 1, delay: Optional[float] = None, md: Optional[Dict[str, Any]] = None
):
    return (yield from count(detectors, num=num, delay=delay, md=md))
