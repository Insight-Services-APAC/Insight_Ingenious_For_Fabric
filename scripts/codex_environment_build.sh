uv venv
source .venv/bin/activate
uv pip install -r pyproject.toml
pip install ms-fabric-cli
fab config set encryption_fallback_enabled true 
fab auth login -u ${FABRIC_SERVICE_PRINCIPAL_ID} -p ${FABRIC_SERVICE_PRINCIPAL_SECRET} --tenant ${FABRIC_TENANT_ID}