from pyspark.sql import SparkSession
from utils import *

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("CustomSchemaDataFrame").getOrCreate()

    # Sample data
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]

    # Column names
    columns = ['log_id', 'user_id', 'action', 'timestamp']

    # Create DataFrame with custom schema
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    df.show()

    # Process data
    process_data(spark, df)
