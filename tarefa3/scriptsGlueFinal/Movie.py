from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
from datetime import datetime

# Inicialize o contexto do Spark e o contexto Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Crie um job Glue
job = Job(glueContext)
job.init("historical-load-job")

# Leia os dados CSV da Raw Zone
raw_data = spark.read.option("delimiter", "|").csv("s3://theclown/raw/movies.csv", header=True, encoding="UTF-8", nullValue="")

# Faça o processamento necessário nos dados
processed_data = raw_data.select(
    col("id").cast("string").alias("id"),
    col("tituloPincipal").cast("string").alias("titulo_principal"),
    col("tituloOriginal").cast("string").alias("titulo_original"),
    col("anoLancamento").cast("int").alias("ano_lancamento"),
    col("tempoMinutos").cast("int").alias("tempo_minutos"),
    col("genero").cast("string"),
    col("notaMedia").cast("float").alias("nota_media"),
    col("numeroVotos").cast("int").alias("numero_votos"),
    col("generoArtista").cast("string").alias("genero_artista"),
    col("personagem").cast("string"),
    col("nomeArtista").cast("string").alias("nome_artista"),
    col("anoNascimento").cast("int").alias("ano_nascimento"),
    col("anoFalecimento").cast("int").alias("ano_falecimento"),
    col("profissao").cast("string").alias("profissao"),
    col("titulosMaisConhecidos").cast("string").alias("titulos_mais_conhecidos")
)
# Obter a data atual para criar a estrutura de pastas
current_date = datetime.now().strftime("%Y-%m-%d")

# Caminho de destino na Trusted Zone com a estrutura de pastas usando a data atual
target_path = f"s3://theclown/raw/trusted/movies_series/movies/{current_date}"

# Grave os dados processados no formato Parquet na Trusted Zone com a estrutura de pastas
processed_data.write.mode("overwrite").parquet(target_path)

# Encerre o job
job.commit()

