import concurrent.futures
import logging
import re
import subprocess

import pandas as pd
from rich.console import Console
from rich.logging import RichHandler

# Set up rich logging
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console)],
)
log = logging.getLogger("rich")

log.info("Starting the script...")

ws = "APACServices_DEV.Workspace"

# Step 1: List lakehouses
command = ["fab", "ls", ws]
try:
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    output = result.stdout
    log.info("Successfully fetched lakehouses.")
except subprocess.CalledProcessError as e:
    log.error(f"Error running the command: {e}")
    exit()

# Filter the output to get only the lines that contain "lakehouse."
lakehouses = [line for line in output.split("\n") if "lakehouse." in line]
if not lakehouses:
    log.warning("No lakehouses found.")
    log.debug(f"Command output:\n{output}")
    exit()

for lakehouse in lakehouses:
    log.info(f"Processing lakehouse: {lakehouse}")

    # Step 2: List tables in the lakehouse
    command = ["fab", "ls", f"{ws}/{lakehouse}/Tables"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        output = result.stdout
        log.info(f"Successfully fetched tables for lakehouse: {lakehouse}")
    except subprocess.CalledProcessError as e:
        log.error(f"Error running table listing for lakehouse {lakehouse}: {e}")
        continue

    tables = output.split("\n")
    if not tables:
        log.warning(f"No tables found for lakehouse: {lakehouse}")
        continue

    # Step 3: Fetch table schemas in parallel
    def fetch_table_schema(table):
        command = ["fab", "table", "schema", f"{ws}/{lakehouse}/Tables/{table}"]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return table, result.stdout
        except subprocess.CalledProcessError as e:
            log.error(f"Error fetching schema for table {table}: {e}")
            log.error(e.stdout)
            return table, None

    log.info(f"Fetching schemas for tables in lakehouse: {lakehouse}")
    if len(tables) > 0:
        max_threads = 5  # Limit the number of threads
        with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
            futures = {
                executor.submit(fetch_table_schema, table): table for table in tables
            }
            for future in concurrent.futures.as_completed(futures):
                try:
                    table, output = future.result()
                    if output:
                        log.info(f"Schema for table {table}:\n{output}")
                    else:
                        log.error(f"Failed to fetch schema for table {table}.")
                except Exception as e:
                    log.error(f"Error processing table: {e}")


# Step 4: Parse schema result
def parse_schema_result(output):
    log.info("Parsing schema result...")
    lines = output.split("\n")

    if not lines or len(lines) < 2:
        log.warning("No data returned from the command.")
        exit()

    if not re.match(r"\* Schema extracted successfully", lines[1]):
        log.error("Schema extraction failed or unexpected output.")
        exit()

    data_lines = lines[4:]
    table = []
    for line in data_lines:
        columns = re.split(r"\s{2,}", line.strip())
        if len(columns) == 2:
            table.append({"Name": columns[0], "Type": columns[1]})

    df = pd.DataFrame(table)
    log.info("Schema parsed successfully.")
    console.print(df.to_string(index=False), style="green")

    df.to_csv("output.csv", index=False)
    log.info("Schema saved to output.csv.")
