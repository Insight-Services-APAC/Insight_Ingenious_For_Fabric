from ingen_fab.notebook_utils.fabric_code_tester import FabricCodeTester

# Create an instance of FabricCodeTester
tester = FabricCodeTester()

# Define the code to be tested
code_to_test = """
def add(a, b):
    return a + b
result = add(5, 3)
"""

# Call the test_code method with the code to be tested
tester.test_code(code_to_test)