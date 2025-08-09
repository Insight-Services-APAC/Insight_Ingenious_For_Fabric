import asyncio

import numpy as np


async def random_delay() -> None:
    """
    Adds a short random delay before logging operations.
    This helps reduce contention when multiple parallel operations
    try to write to the same Delta table at the same time.
    """
    # Random delay between 0.1 and 1.5 seconds
    delay = 0.1 + np.random.random() * 1.4
    await asyncio.sleep(delay)
