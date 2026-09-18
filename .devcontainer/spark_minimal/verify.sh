#!/usr/bin/env bash
# Verifies the spark_minimal dev container from inside it.
#
#   bash .devcontainer/spark_minimal/verify.sh          # toolchain, CLI, unit tests, then the
#                                                       # Spark-backed lakehouse_utils tests
#   bash .devcontainer/spark_minimal/verify.sh quick    # same, but a single Delta write/read
#                                                       # through the library's session factory
#                                                       # instead of the full test file
#
# Exit code is non-zero on the first failure. The first Spark session fetches the Delta jars
# from Maven into the ingen-fab-ivy volume, so it needs internet once. The script relies on the
# environment the container provides (FABRIC_ENVIRONMENT, PATH); it does not set anything
# itself, so a container that fails to provide them fails here. Full mode ends with the
# lakehouse_utils test file, whose last test drops every table under <repo>/tmp/spark/Tables/.
set -euo pipefail

# Every step is relative to the repository root, whatever directory the script is called from.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

mode="${1:-full}"
step() { printf '\n=== %s\n' "$1"; }

step "toolchain"
python --version
java -version
uv --version
odbcinst -q -d
echo "venv python: $(command -v python)"
[ "${FABRIC_ENVIRONMENT:-}" = "local" ] || { echo "FABRIC_ENVIRONMENT is '${FABRIC_ENVIRONMENT:-}', expected 'local' from the container"; exit 1; }
python -c "import ingen_fab, pyspark, fabric_cicd; print('ingen_fab importable, pyspark', pyspark.__version__)"

step "CLI"
ingen_fab --help >/dev/null && echo "ingen_fab --help ok"

step "offline unit tests (fabric-cicd wrapper)"
python -m pytest tests/test_promotion_utils.py -q -p no:cacheprovider

if [ "$mode" = "quick" ]; then
  step "Spark + Delta round trip through the library's session factory (quick mode)"
  python - <<'PY'
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# In the local environment lakehouse_utils builds its own Delta-enabled SparkSession; this is
# the code path the generated notebooks use, not a copy of it.
lh = lakehouse_utils("verify-workspace", "verify-lakehouse")
spark = lh.get_connection
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
df.write.format("delta").mode("overwrite").save("/tmp/verify_delta")
n = spark.read.format("delta").load("/tmp/verify_delta").count()
version = spark.version
spark.stop()
assert n == 2, n
print("spark", version, "ok via lakehouse_utils, delta rows:", n)
PY
  echo; echo "verify.sh (quick): all steps passed"; exit 0
fi

# LIB resolves to test_<lib>_pytest.py; the test_ prefix is optional.
step "Spark-backed library tests (ingen_fab test local pyspark lakehouse_utils)"
ingen_fab test local pyspark lakehouse_utils

echo; echo "verify.sh: all steps passed"
