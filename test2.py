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

from ingen_fab.fabric_cicd.promotion_utils import promotion_utils
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


vlu = VariableLibraryUtils(
    project_path="./sample_project",
    environment="development",
    template_path="ingen_fab/ddl_scripts/_templates/warehouse/config.py.jinja",
    output=None,
    in_place=False,
)


vlu.inject_variables_into_template()
