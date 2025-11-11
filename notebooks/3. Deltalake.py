# Databricks notebook source
# overwrite com inferschema
carros = spark.read.load("/Volumes/workspace/default/dados/Carros.csv",
                     format="csv", sep=";", inferSchema=True, header=True)
carros.write.mode("overwrite").saveAsTable("carros")                     

# COMMAND ----------

# MAGIC %%sql
# MAGIC -- carros já existe
# MAGIC select * from carros

# COMMAND ----------

from pyspark.sql.functions import col, when
carros_df = spark.table("carros")
# nome da pasta
carros_df.write.format("delta").save("/Volumes/workspace/default/dados/carros_delta")

# COMMAND ----------

delta_df = spark.read.format("delta").load("/Volumes/workspace/default/dados/carros_delta")
delta_df.show()

# COMMAND ----------

#atualiza tipo de motor
delta_df = delta_df.withColumn('TipoMotor',when(col('TipoMotor') == 0, 2).otherwise(col('TipoMotor')))

# COMMAND ----------

#grava, carrega e exibe
delta_df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/dados/carros_delta")
updated_delta_df = spark.read.format("delta").load("/Volumes/workspace/default/dados/carros_delta")
updated_delta_df.show()

# COMMAND ----------

# MAGIC %%sql
# MAGIC SELECT version, timestamp, operation, operationMetrics
# MAGIC FROM (
# MAGIC   DESCRIBE HISTORY delta.`/Volumes/workspace/default/dados/carros_delta`
# MAGIC )
# MAGIC

# COMMAND ----------

# "rollback" da tabela
version0_df = spark.read.format("delta").option("versionAsOf", 0).load("/Volumes/workspace/default/dados/carros_delta")
version0_df.show()

# COMMAND ----------

#versao atual continua
updated_delta_df.show()

# COMMAND ----------

version_default = spark.read.format("delta").load("/Volumes/workspace/default/dados/carros_delta")
version_default.show()

# COMMAND ----------

#apaga delta
dbutils.fs.rm("/Volumes/workspace/default/dados/carros_delta", recurse=True)

# COMMAND ----------

