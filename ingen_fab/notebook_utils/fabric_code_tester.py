import json

import requests
from azure.identity import DefaultAzureCredential


class FabricCodeTester:
    def __init__(self):
        """
        Initializes the FabricCodeTester class.
        This class is used to test code execution in Azure Fabric using a user-defined function (UDF).
        """
        pass

    def test_code(self, code: str):
        # Acquire a token using DefaultAzureCredential
        credential = DefaultAzureCredential()
        scope = "https://analysis.windows.net/powerbi/api/.default"  # Note: Use .default for client credentials flow
        token = credential.get_token(scope)

        if not token.token:
            print("Error:", "Could not get access token")

        # Prepare headers
        headers = {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

        endpoint = "https://api.fabric.microsoft.com/v1/workspaces/5555c1a9-026a-4f3c-9c85-cdd000943d7f/userDataFunctions/8973e4c3-c2b2-4f76-81cb-fa41a454666a/functions/codex_tester/invoke"

        # UPDATE HERE: Update the request body based on the inputs to your function
        request_body = {"code": code}

        # Invoke fabric udf function from this app
        response = requests.post(endpoint, json=request_body, headers=headers)

        # Check the response status code
        if response.status_code == 200:
            print("Function invoked successfully!")
            data = response.json()
            print(json.dumps(data, indent=4))
        else:
            print("Failed to invoke function. Status code:", response.status_code)
            print("Response:", response.text)
