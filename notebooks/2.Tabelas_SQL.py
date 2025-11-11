# Databricks notebook source
despachantes = spark.read.load("/Volumes/workspace/default/dados/despachantes.csv",
                     format="csv", sep=",", inferSchema=True, header=False)
despachantes.show()

# COMMAND ----------

despachantes.write.saveAsTable("Despachantes")

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT * FROM Despachantes