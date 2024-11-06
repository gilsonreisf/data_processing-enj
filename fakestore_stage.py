import requests
import pandas as pd
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


glueContext = GlueContext(SparkContext.getOrCreate())
spark = SparkSession.builder.getOrCreate()
s3 = boto3.client('s3')
glue = boto3.client('glue')


bucket_name = "enjoei-elo"
paths = {
    "users": "path/to/stage/users/",
    "carts": "path/to/stage/carts/",
    "products": "path/to/stage/products/"
}


database_name = "db_stage"
table_names = {
    "users": "users",
    "carts": "carts",
    "products": "products"
}


def delete_existing_data(database_name, table_name, s3_path):
 
    try:
        glue.delete_table(DatabaseName=database_name, Name=table_name)
        print(f"Deletando tabela pré-existente: {database_name}.{table_name}")
    except glue.exceptions.EntityNotFoundException:
        print(f"Tabela {database_name}.{table_name} não encontrada.")


    s3_bucket = boto3.resource('s3').Bucket(bucket_name)
    s3_bucket.objects.filter(Prefix=s3_path).delete()
    print(f"Deletando dados existentes em s3://{bucket_name}/{s3_path}")


def fetch_data(url):
    try:
        response = requests.get(url, timeout=10, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        print(f"Erro encontrado: {err} - {url}")
        return None

def save_to_s3_and_catalog(df, s3_path, table_name):
 
    spark_df = spark.createDataFrame(df)

    spark_df.write.mode("overwrite").parquet(f"s3://{bucket_name}/{s3_path}")
    

    dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, table_name)
    glueContext.write_dynamic_frame.from_catalog(
        frame=dynamic_frame,
        database=database_name,
        table_name=table_name,
        additional_options={"path": f"s3://{bucket_name}/{s3_path}"}
    )


collections = {
    "users": "https://fakestoreapi.com/users",
    "carts": "https://fakestoreapi.com/carts",
    "products": "https://fakestoreapi.com/products"
}

for name, url in collections.items():
    data = fetch_data(url)
    if data:
        df = pd.DataFrame(data)
 
        delete_existing_data(database_name, table_names[name], paths[name])
        save_to_s3_and_catalog(df, paths[name], table_names[name])

print("Carga de users, carts e products executada com sucesso.")
