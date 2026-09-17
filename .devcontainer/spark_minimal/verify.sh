#!/usr/bin/env bash
# Verifies the spark_minimal dev container from inside it.
#
#   bash .devcontainer/spark_minimal/verify.sh          # everything
#   bash .devcontainer/spark_minimal/verify.sh quick    # skip the Spark-backed library tests
#
# Exit code is non-zero on the first failure. Needs internet once: the first Spark session
# fetches the Delta jars from Maven.
set -euo pipefail

mode="${1:-full}"
step() { printf '\n=== %s\n' "$1"; }

step "toolchain"
python --version
java -version 2>&1 | head -1
uv --version
odbcinst -q -d | head -1
echo "venv python: $(command -v python)"
python -c "import ingen_fab, pyspark, fabric_cicd; print('ingen_fab importable, pyspark', pyspark.__version__)"

step "CLI"
FABRIC_ENVIRONMENT=local ingen_fab --help >/dev/null && echo "ingen_fab --help ok"

step "offline unit tests (fabric-cicd wrapper)"
python -m pytest tests/test_promotion_utils.py -q -p no:cacheprovider

step "Spark + Delta round trip"
python - <<'PY'
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
b = (SparkSession.builder.appName("verify").master("local[2]")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
df.write.format("delta").mode("overwrite").save("/tmp/verify_delta")
n = spark.read.format("delta").load("/tmp/verify_delta").count()
spark.stop()
assert n == 2, n
print("spark ok, delta rows:", n)
PY

if [ "$mode" = "quick" ]; then
  echo; echo "quick mode: skipping Spark-backed library tests"; exit 0
fi

# LIB resolves to test_<lib>_pytest.py; the test_ prefix is optional.
step "Spark-backed library tests (ingen_fab test local pyspark lakehouse_utils)"
FABRIC_ENVIRONMENT=local ingen_fab test local pyspark lakehouse_utils

echo; echo "verify.sh: all steps passed"
