from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime

# Inicialize o contexto do Spark e o contexto Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Crie um job Glue
job = Job(glueContext)
job.init("tmdb-load-job")

# Leia os dados do TMDB da Raw Zone no formato JSON
raw_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://theclown/raw/tmdb/json/2024/01/27/tmdb_data_37854.json"]},
    format="json"
)

# Selecione apenas as colunas desejadas
processed_data = raw_data.select_fields(["name", "first_air_date", "popularity", 
                                         "production_companies", "vote_average", 
                                         "episode_run_time", "genres", 
                                         "origin_country", "season_number", 
                                         "networks", "air_date"])

# Converta o DynamicFrame para DataFrame
processed_df = processed_data.toDF()

# Obtenha a data atual para criar a estrutura de pastas
current_date = datetime.now().strftime("%Y-%m-%d")

# Construa o caminho de destino na Trusted Zone com a estrutura de pastas
target_path = f"s3://theclown/raw/trusted/tmdb/{current_date}"

# Grave os dados processados no formato Parquet na Trusted Zone, no modo append
processed_df.write.mode("append").parquet(target_path)

# Encerre o job
job.commit()

