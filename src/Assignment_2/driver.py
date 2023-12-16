from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from utils import *

# Create a SparkSession
spark = SparkSession.builder.appName("CreditCardData").getOrCreate()

# Define the dataset
data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

# Define the schema for the DataFrame
schema = ["card_number"]

# Create DataFrame using createDataFrame method
credit_card_df = create_credit_card_dataframe(spark, data, schema)

# Show the DataFrame
credit_card_df.show()

# Get the number of partitions
num_partitions = credit_card_df.rdd.getNumPartitions()
print("Number of partitions:", num_partitions)

# Repartition the DataFrame to increase the partition size to 5
increase_credit_card_df = increase_partitions(credit_card_df, 5)
increase_num_partitions = increase_credit_card_df.rdd.getNumPartitions()
print("Number of increased_partitions:", increase_num_partitions)

# Reduce the partition size back to its original size
back_to_og_credit_card_df = back_to_original_partitions(increase_credit_card_df, num_partitions)
back_num_partitions = back_to_og_credit_card_df.rdd.getNumPartitions()
print("Number of back_to_og_partitions:", num_partitions)

# Save DataFrame to disk in Parquet format
parquet_path = r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q2"
save_and_read_parquet(credit_card_df, parquet_path)

# Create a UDF to print only last 4 digits marking remaining digits as *
mask_card_udf = mask_card_number_udf()

# Apply the UDF to create a new column 'masked_card_number' and show the DataFrame
show_masked_credit_card(credit_card_df, mask_card_udf)



