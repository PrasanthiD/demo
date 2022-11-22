#from pyspark.sql import SparkSession
from pyspark.sql import SparkSession,Row
def func():
    spark = SparkSession.builder.appName("demo").getOrCreate()
    df = spark.read.csv("s3://practice-bigdata-services/source/emp_demo_data.csv",header=True,inferSchema=True)
    df.printSchema
    df.columns
    df = df.filter("salary < 800")

    print("*********************************************************************************")
    spark.sql("show databases").show()
    print("*********************************************************************************")
    df.write.option("header","true").format("csv").option("path","s3://practice-bigdata-services/target/emp").saveAsTable("demo.emp")

if __name__ == '__main__':
    func()