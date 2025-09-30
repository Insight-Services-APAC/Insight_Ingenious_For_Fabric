from dataclasses import dataclass
from datetime import datetime


@dataclass
class StatusEntry:
    """Represents a status log entry for a module."""

    timestamp: datetime
    module: str
    item_type: str
    item_name: str | None
    status: str


def read_log_status_entries(file_path: str) -> list[StatusEntry]:
    """
    Reads a log file and extracts unique status entries per module.

    Only captures log lines that end with '->Status.'

    Args:
        file_path: The path to the log file.

    Returns:
        A list of StatusEntry objects with no duplicates per module.
    """
    status_entries = []
    seen_modules = set()

    try:
        with open(file_path, "r", encoding="utf-8") as log_file:
            for line in log_file:
                line = line.strip()

                # Check if line contains status indicator
                if " - ->" not in line:
                    continue

                if " - ->->" in line:
                    continue

                # Extract the status part (after the last ->)
                status_part = line.split(" - ->")[-1].strip()

                # Remove the period and get the status
                status = status_part

                # Skip lines that are just "->."
                if not status:
                    continue

                try:
                    # Parse the full log entry
                    parts = line.split(" - ", 3)
                    if len(parts) >= 4:
                        timestamp_str = parts[0]
                        level = parts[1]  # noqa: F841
                        module = parts[2]
                        message = parts[3]

                        # Skip if we've already seen this module
                        if module in seen_modules:
                            continue

                        # Parse timestamp
                        timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S,%f")

                        # Extract item type and name from message
                        item_type = None
                        item_name = None

                        if "Publishing" in message:
                            # Extract from patterns like "Publishing Notebook 'name'"
                            publish_parts = message.split("Publishing", 1)[1].strip()
                            if " '" in publish_parts:
                                type_and_name = publish_parts.split(" '", 1)
                                item_type = type_and_name[0].strip()
                                if len(type_and_name) > 1:
                                    item_name = type_and_name[1].rstrip("'")
                            else:
                                # Handle cases like "Publishing Workspace Folders"
                                item_type = publish_parts.split("->")[0].strip()

                        entry = StatusEntry(
                            timestamp=timestamp,
                            module=module,
                            item_type=item_type or "Unknown",
                            item_name=item_name,
                            status=status,
                        )

                        status_entries.append(entry)
                        seen_modules.add(module)

                except (ValueError, IndexError):
                    continue

    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return []
    except Exception as e:
        print(f"An error occurred while reading the log file: {e}")
        return []

    return status_entries


def print_status_summary(status_entries: list[StatusEntry]) -> None:
    """
    Prints a formatted summary of status entries.

    Args:
        status_entries: List of StatusEntry objects to summarize.
    """
    if not status_entries:
        print("No status entries found.")
        return

    print(f"Found {len(status_entries)} unique module status entries:\n")
    print(f"{'Module':<50} {'Item Type':<20} {'Item Name':<40} {'Status':<15}")
    print("-" * 125)

    for entry in status_entries:
        item_name = entry.item_name or "N/A"
        print(f"{entry.module:<50} {entry.item_type:<20} {item_name:<40} {entry.status:<15}")
