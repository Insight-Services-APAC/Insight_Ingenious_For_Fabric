from ingen_fab.notebook_utils.fabric_cli_notebook import FabricLivyNotebook

# Initialize with workspace and lakehouse IDs
livy_notebook = FabricLivyNotebook(
    workspace_id="5555c1a9-026a-4f3c-9c85-cdd000943d7f",
    lakehouse_id="352eb28d-d085-4767-a985-28b03d0829ae",
)

# Option 1: Run simple code
result = livy_notebook.run_notebook_code("print('Hello from Livy!)")

# Option 2: Run with template error handling
# result = livy_notebook.run_template_notebook("print('Hello with error handling!')")

# Check results
print("Result:", result)
