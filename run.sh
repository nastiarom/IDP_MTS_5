#!/bin/bash

JUMP_NODE=$1
NAME_NODE=$2
USER=$3

ssh "$USER@$JUMP_NODE" << EOF
        sudo -i -u hadoop bash << 'HADOOP'

        export HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.0/etc/hadoop"
        export HIVE_HOME="/home/hadoop/apache-hive-4.0.0-bin"
        export HIVE_CONF_DIR="\$HIVE_HOME/conf"
        export HIVE_AUX_JARS_PATH="\$HIVE_HOME/lib/*"
        export PATH="\$PATH:\$HIVE_HOME/bin"
        export SPARK_LOCAL_IP="$JUMP_NODE"
        export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*"
        export SPARK_HOME="/home/hadoop/spark-3.5.3-bin-hadoop3/"
        export PYTHONPATH=\$(find \$SPARK_HOME/python/lib -name "*.zip" | tr '\n' ':'):\$PYTHONPATH
        export PATH="\$SPARK_HOME/bin:\$PATH"

        cd /home/hadoop/spark-3.5.3-bin-hadoop3/

        python3 -m venv .venv
        source .venv/bin/activate
        pip install -U pip
        pip install ipython

        ipython -c "%logstart"

        python3 - << 'PYTHON'
from pyspark.sql import SparkSession
from onetl.connection import SparkHDFS
from onetl.file import FileDFReader
from onetl.connection import Hive
from onetl.file.format import CSV
from onetl.db import DBWriter
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("yarn") \
    .appName("spark-with-yarn") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hive.metastore.uris", "thrift://jn:9083") \
    .enableHiveSupport() \
    .getOrCreate()

hdfs = SparkHDFS(host="$NAME_NODE", port=9000, spark=spark, cluster="test")
reader = FileDFReader(connection=hdfs, format=CSV(delimiter="\t", header=True), source_path="/input")
df = reader.run(["for_spark.csv"])

df = df.withColumn("reg_year", F.col("registration date").substr(0, 4))

hive = Hive(spark=spark, cluster="test")
writer = DBWriter(connection=hive, table="test.spark_parts", options={"if_exists": "replace_entire_table"})
writer.run(df)

writer = DBWriter(connection=hive, table="test.one_parts", options={"if_exists": "replace_entire_table"})
writer.run(df.coalesce(1))

writer = DBWriter(connection=hive, table="test.hive_parts", options={"if_exists": "replace_entire_table", "partitionBy": "reg_year"})
writer.run(df)

spark.stop()
PYTHON

        ipython -c "%logstop"

        pip install prefect

        cat << 'PREFECT' > ipython_log.py
from pyspark.sql import SparkSession
from onetl.connection import SparkHDFS
from onetl.file import FileDFReader
from onetl.connection import Hive
from onetl.file.format import CSV
from onetl.db import DBWriter
from pyspark.sql import functions as F
from prefect import flow, task

@task
def get_spark():
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("spark-with-yarn") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hive.metastore.uris", "thrift://jn:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

@task
def stop_spark(spark):
    spark.stop()

@task
def extract(spark):
    hdfs = SparkHDFS(host="$NAME_NODE", port=9000, spark=spark, cluster="test")
    reader = FileDFReader(connection=hdfs, format=CSV(delimiter="\t", header=True), source_path="/input")
    df = reader.run(["for_spark.csv"])
    return df

@task
def transform(df):
    df = df.withColumn("reg_year", F.col("registration date").substr(0, 4))
    return df

@task
def load(spark, df):
    hive = Hive(spark=spark, cluster="test")
    writer = DBWriter(connection=hive, table="test.hive_parts", options={"if_exists": "replace_entire_table", "partitionBy": "reg_year"})
    writer.run(df)

@flow
def process_data():
    spark = get_spark()
    df = extract(spark)
    df = transform(df)
    load(spark, df)
    stop_spark(spark)

if __name__ == "__main__":
    process_data()
PREFECT
        python3 ipython_log.py

        HADOOP
    JUMP
EOF