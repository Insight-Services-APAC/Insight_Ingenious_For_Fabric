#from ingen_fab.notebook_utils.fabric_code_tester import FabricCodeTester

# Create an instance of FabricCodeTester
#tester = FabricCodeTester()

# Define the code to be tested
#code_to_test = """
#def add(a, b):
#    return a + b
#result = add(5, 3)
# """

# Call the test_code method with the code to be tested
# tester.test_code(code_to_test)

from ingen_fab.python_libs.python.promotion_utils import promotion_utils

workspace_id="5555c1a9-026a-4f3c-9c85-cdd000943d7f"
lakehouse_id="352eb28d-d085-4767-a985-28b03d0829ae"

pu = promotion_utils(
    workspace_id=workspace_id,
    repository_directory="./sample_project/fabric_workspace_items",
    item_type_in_scope=["Warehouse"],
    environment="development"
)

pu.publish_all()
