# Databricks notebook source
# MAGIC %%sql
# MAGIC SHOW CATALOGS;
# MAGIC -- Usamos catalogo workspace

# COMMAND ----------

# MAGIC %%sql
# MAGIC SHOW VOLUMES;
# MAGIC --criamos volumes em default

# COMMAND ----------

# MAGIC %%sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.dados;
# MAGIC --o caminho do volume:
# MAGIC --/Volumes/workspace/default/dados
# MAGIC --agora podemos ir catalog e import todos os dados para este volume
# MAGIC

# COMMAND ----------

#importar dados definindo schema
#vamos deixar a data como string de propósito
from pyspark.sql.types import *
arqschema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
#o caminho do arquivo no volume
despachantes = spark.read.csv("/Volumes/workspace/default/dados/despachantes.csv", header=False, schema=arqschema)
despachantes.show()

# COMMAND ----------

#outro exemplo, inferindo schema, usando load e informado tipo
desp_autoschema = spark.read.load("/Volumes/workspace/default/dados/despachantes.csv",
                     format="csv", sep=",", inferSchema=True, header=False)
desp_autoschema.show()

# COMMAND ----------

#comparando os schemas, outra forma
desp_autoschema.printSchema()
despachantes.printSchema()

# COMMAND ----------

from pyspark.sql import functions as Func
#condição lógica com where
despachantes.select("id","nome","vendas").where(Func.col("vendas") > 20).show()
#& para and, | para or, e ~ para not
despachantes.select("id","nome","vendas").where((Func.col("vendas") > 20) & (Func.col("vendas") < 40)).show()

# COMMAND ----------

#renomear coluna
novodf = despachantes.withColumnRenamed("nome","nomes")
novodf.columns

# COMMAND ----------

from pyspark.sql.functions import *
#coluna data está como string, vamos transformar em data
despachantes2 = despachantes.withColumn("data2", to_timestamp(Func.col("data"),"yyyy-MM-dd"))
despachantes2.printSchema()

# COMMAND ----------

#operações sobre datas
despachantes2.select(year("data")).show()
despachantes2.select(year("data")).distinct().show()
despachantes2.select("nome",year("data")).orderBy("nome").show()
despachantes2.select("data").groupBy(year("data")).count().show()
despachantes2.select(Func.sum("vendas")).show()

# COMMAND ----------

# salvar tipos, são diretório
despachantes.write.mode("overwrite").format("parquet").save("/Volumes/workspace/default/dados/dfimportparquet")
despachantes.write.mode("overwrite").format("csv").save("/Volumes/workspace/default/dados/dfimportcsv")
despachantes.write.mode("overwrite").format("json").save("/Volumes/workspace/default/dados/dfimportjson")
despachantes.write.mode("overwrite").format("orc").save("/Volumes/workspace/default/dados/dfimportorc")
# podemos confirmar os dados olhando no volume

# COMMAND ----------

#ler dados
par = spark.read.format("parquet").load("/Volumes/workspace/default/dados/dfimportparquet/*.parquet")
par.show()
par.printSchema()

# COMMAND ----------

#formato tabular
despachantes.show(1)

# COMMAND ----------

#formato de lista
despachantes.take(2) #/head/firt

# COMMAND ----------

#retorna todos dados como uma lista
despachantes.collect()

# COMMAND ----------

#conta
despachantes.count()

# COMMAND ----------

#transformações
#padrão crescente
despachantes.orderBy("vendas").show()

# COMMAND ----------

#decrescente
despachantes.orderBy(Func.col("vendas").desc()).show()

# COMMAND ----------

#se quiser cidade dec e valor dec
despachantes.orderBy(Func.col("cidade").desc(),Func.col("vendas").desc()).show()

# COMMAND ----------

#agrupar dados
#ver vendas por cidade
despachantes.groupBy("cidade").agg(sum("vendas")).show()

# COMMAND ----------

#ordernar por vendas decrecente
from pyspark.sql import functions as F
despachantes.groupBy("cidade").agg(F.sum("vendas").alias("total_vendas")).orderBy(F.desc("total_vendas")).show()

# COMMAND ----------

#filter
despachantes.filter(Func.col("nome") == "Deolinda Vilela").show()

# COMMAND ----------

