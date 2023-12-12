from pyspark.sql.functions import col, countDistinct

#Find the customers who have bought only product A
def find_customers_bought_only_A(dataframe):
    return dataframe.filter(col('product_model') == 'A')

#Find customers who upgraded from product B to product E
def find_customers_upgraded_B_to_E(dataframe):
    return (dataframe.alias("p1")
            .join(dataframe.alias("p2"), (col("p1.customer") == col("p2.customer")) &
                  (col("p1.product_model") == "B") & (col("p2.product_model") == "E"))
            .select("p1.customer")
            .distinct())

#Find customers who have bought all models in the new Product Data
def find_customers_bought_all_models(purchase_data_df, product_data_df):
    distinct_models = product_data_df.select(countDistinct("product_model")).collect()[0][0]

    return (purchase_data_df
            .groupBy("customer")
            .agg(countDistinct("product_model").alias("distinct_models"))
            .filter(col("distinct_models") == distinct_models)
            .select("customer"))
