from pyspark.sql import SparkSession
from delta import *

def main():
    # Create Spark session with Delta
    spark = configure_spark_with_delta_pip(
        SparkSession.builder
        .appName("ConfigUtilsTest")
        .master("local[*]")
    ).getOrCreate()
    
    # Add your config module
    from ingen_fab.python_libs.common.config_utils import get_config_value
    
    # Test your ConfigUtils
    def test_config():
        
        return get_config_value("fabric_environment")
    
    # Run test
    rdd = spark.sparkContext.parallelize([1])
    result = rdd.map(lambda x: test_config()).collect()
    print("Test result:", result)
    
    spark.stop()

if __name__ == "__main__":
    main()