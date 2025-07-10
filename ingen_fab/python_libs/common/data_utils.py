
from datetime import datetime


def timestamp_now() -> int:
    """Get current UTC timestamp in YYYYMMDDHHMMSSmmm format as integer."""
    return int(datetime.utcnow().strftime('%Y%m%d%H%M%S%f')[:-3])


