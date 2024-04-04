# Databricks notebook source
# MAGIC %pip install langchain langchain-community mlflow setuptools
# MAGIC %pip install pyspark_ai

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from langchain_community.chat_models import ChatDatabricks
from pyspark_ai import SparkAI

# COMMAND ----------

llm = ChatDatabricks(endpoint="databricks-dbrx-instruct")

spark_ai = SparkAI(llm=llm)

spark_ai.activate() #activate partial functions for spark dataframe

df = spark_ai._spark.createDataFrame(
    [
        ("Normal", "Cellphone", 6000),
        ("Normal", "Tablet", 1500),
        ("Mini", "Tablet", 5500),
        ("Mini", "Cellphone", 5000),
        ("Foldable", "Cellphone", 6500),
        ("Foldable", "Tablet", 2500),
        ("Pro", "Cellphone", 3000),
        ("Pro", "Tablet", 4000),
        ("Pro Max", "Cellphone", 4500)
    ],
    schema=["product", "category", "revenue"]
)

# COMMAND ----------

# DataFrame transformations
df.ai.transform("What are the best-selling and the second best-selling products in every category?").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, product, revenue,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
# MAGIC     FROM spark_ai_temp_view__995849735

# COMMAND ----------

_sqldf.display()
