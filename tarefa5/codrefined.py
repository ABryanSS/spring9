from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

# Inicialização do SparkContext
sc = SparkContext()

# Inicialização do GlueContext e SparkSession
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Esquema para o arquivo "artistas"
schema_artistas = StructType([
    StructField("idartista", IntegerType(), True),
    StructField("nomeartista", StringType(), True),
    StructField("personagem", StringType(), True),
    StructField("anonascimento", IntegerType(), True),
    StructField("anofalecimento", IntegerType(), True),
    StructField("profissao", StringType(), True),
    StructField("titulosmaisconhecidos", StringType(), True),
    StructField("genero_artista", StringType(), True)
])

# Esquema para o arquivo "fatofilmes"
schema_fatofilmes = StructType([
    StructField("idfilme", IntegerType(), True),
    StructField("idartista", IntegerType(), True),
    StructField("notamedia", FloatType(), True),
    StructField("numerovotos", IntegerType(), True),
    StructField("tempominutos", IntegerType(), True)
])

# Esquema para o arquivo "fatoseries"
schema_fatoseries = StructType([
    StructField("idserie", IntegerType(), True),
    StructField("idartista", IntegerType(), True),
    StructField("notamedia", FloatType(), True),
    StructField("numerovotos", IntegerType(), True),
    StructField("tempominutos", IntegerType(), True)
])

# Esquema para o arquivo "infodados"
schema_infodados = StructType([
    StructField("id_genres", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("voteaverage", DoubleType(), True),
    StructField("originallanguage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("firstairdate", StringType(), True),
    StructField("numberofepisodes", IntegerType(), True)
])

# Esquema para o arquivo "infofilmes"
schema_infofilmes = StructType([
    StructField("idfilme", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("tituloprincipal", StringType(), True),
    StructField("titulooriginal", StringType(), True),
    StructField("anolancamento", IntegerType(), True),
    StructField("genero", StringType(), True)
])

# Esquema para o arquivo "infoseries"
schema_infoseries = StructType([
    StructField("idserie", IntegerType(), True),
    StructField("id", StringType(), True),
    StructField("tituloprincipal", StringType(), True),
    StructField("titulooriginal", StringType(), True),
    StructField("anolancamento", IntegerType(), True),
    StructField("anotermino", IntegerType(), True),
    StructField("genero", StringType(), True)
])

# Lendo os dados dos arquivos GZip e aplicando o esquema definido
dados_artistas = spark.read.option("compression", "gzip").schema(schema_artistas).csv(
    "s3://theclown/raw/trusted/movies_series/series/artisSeries/*.gz")
dados_fatofilmes = spark.read.option("compression", "gzip").schema(schema_fatofilmes).csv(
    "s3://theclown/raw/trusted/movies_series/movies/FatoFilmes/*.gz")
dados_fatoseries = spark.read.option("compression", "gzip").schema(schema_fatoseries).csv(
    "s3://theclown/raw/trusted/movies_series/series/FatoSeries/*.gz")
dados_infodados = spark.read.option("compression", "gzip").schema(schema_infodados).json(
    "s3://theclown/raw/trusted/tmdb/infodados/*.gz")
dados_infofilmes = spark.read.option("compression", "gzip").schema(schema_infofilmes).csv(
    "s3://theclown/raw/trusted/movies_series/movies/infoFilmes/*.gz")
dados_infoseries = spark.read.option("compression", "gzip").schema(schema_infoseries).csv(
    "s3://theclown/raw/trusted/movies_series/series/infoSeries/*.gz")

# Escrevendo os dados em arquivos Parquet
dados_artistas.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/artistas_parquet")
dados_fatofilmes.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/fatofilmes_parquet")
dados_fatoseries.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/fatoseries_parquet")
dados_infodados.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/infodados_parquet")
dados_infofilmes.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/infofilmes_parquet")
dados_infoseries.write.mode("overwrite").parquet("s3://theclown/raw/trusted/refined/infoseries_parquet")

