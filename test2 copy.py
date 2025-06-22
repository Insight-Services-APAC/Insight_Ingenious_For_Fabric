# from ingen_fab.notebook_utils.fabric_code_tester import FabricCodeTester

# Create an instance of FabricCodeTester
# tester = FabricCodeTester()

# Define the code to be tested
# code_to_test = """
# def add(a, b):
#    return a + b
# result = add(5, 3)
# """

# Call the test_code method with the code to be tested
# tester.test_code(code_to_test)

from ingen_fab.python_libs.python.promotion_utils import promotion_utils
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


vlu = VariableLibraryUtils(
    project_path="./sample_project",
    environment="development",
    template_path="ingen_fab/ddl_scripts/_templates/warehouse/config.py.jinja",
    output=None,
    in_place=False
)


vlu.inject_variables_into_template()

workspace_id = "3a4fc13c-f7c5-463e-a9de-57c4754699ff"
lakehouse_id = "352eb28d-d085-4767-a985-28b03d0829ae"

pu = promotion_utils(
    workspace_id=workspace_id,
    repository_directory="./sample_project/fabric_workspace_items",
    item_type_in_scope=[
        "VariableLibrary",
        "DataPipeline",
        "Environment",
        "Notebook",
        "Report",
        "SemanticModel",
        "Lakehouse",
        "MirroredDatabase", 
        "CopyJob",
        "Eventhouse",
        "KQLDatabase",
        "KQLQueryset",
        "Reflex",
        "Eventstream",
        "Warehouse",
        "SQLDatabase",
        "KQLDashboard",
        "Dataflow",
    ],
    environment="development",
)

pu.publish_all()
