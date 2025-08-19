# Find your Spark installation
$sparkHome = $env:SPARK_HOME
if (-not $sparkHome) {
    Write-Host "SPARK_HOME not set. Finding Spark..."
    # You might need to adjust this path
}

# Create or edit spark-defaults.conf
$configContent = @"
spark.jars.packages                io.delta:delta-spark_2.13:4.0.0
spark.sql.extensions               io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog    org.apache.spark.sql.delta.catalog.DeltaCatalog
"@

$configPath = "$sparkHome\conf\spark-defaults.conf"
$configContent | Out-File -FilePath $configPath -Encoding UTF8

# Then just run
pyspark --py-files sample_project/fabric_workspace_items/ddl_scripts/Lakehouses/Config/0_orchestrator_Config.Notebook/notebook-content.py
