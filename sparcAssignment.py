from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Spark сессия
spark = SparkSession.builder.appName("ProductCategories").getOrCreate()

# Датафреймы для продуктов и категорий
products_data = [("Product1", "Category1"),
                 ("Product2", "Category1"),
                 ("Product3", "Category2"),
                 ("Product4", "Category3")]
categories_data = [("Category1", "CategoryName1"),
                   ("Category2", "CategoryName2")]

products_df = spark.createDataFrame(products_data, ["Product", "Category"])
categories_df = spark.createDataFrame(categories_data, ["Category", "CategoryName"])

# Левое внешнее соединение между продуктами и категориями
result_df = products_df.join(categories_df, "Category", "left_outer")

# Обработка продуктов без категорий
result_df = result_df.fillna({"CategoryName": ""})

result_df = result_df.select(col("Product"), col("CategoryName").alias("Category"))
result_df.show()