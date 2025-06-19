import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import os
from ingen_fab.notebook_utils.simple_notebook import SimpleFabricNotebook

if __name__ == "__main__":
    ws_id = os.environ.get("FABRIC_WORKSPACE_ID")
    if not ws_id:
        raise SystemExit("FABRIC_WORKSPACE_ID environment variable not set")

    notebook = SimpleFabricNotebook(ws_id)
    try:
        status = notebook.run_and_wait("demo_notebook", "print('hello world')", poll_interval=10, timeout=60)
        print(f"Notebook run completed with status: {status}")
    except Exception as e:
        print(f"Notebook execution failed: {e}")
