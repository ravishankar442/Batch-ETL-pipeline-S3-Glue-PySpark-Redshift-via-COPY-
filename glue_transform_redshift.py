# glue_transform_redshift.py
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
import sys

args = getResolvedOptions(sys.argv, ['S3_INPUT','S3_SILVER','REDSHIFT_COPY_S3_PREFIX'])
s3_input = args['S3_INPUT']
s3_silver = args['S3_SILVER']
copy_prefix = args['REDSHIFT_COPY_S3_PREFIX']  # S3 path Redshift COPY will read from

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.option("header","true").csv(s3_input)
df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
df = df.withColumn("price", F.col("price").cast(DoubleType()))
df = df.withColumn("total_price", F.col("quantity") * F.col("price"))

# dedupe
w = Window.partitionBy('order_id').orderBy(F.desc('order_date'))
df = df.withColumn('_rn', F.row_number().over(w)).filter(F.col('_rn') == 1).drop('_rn')

# write parquet to silver (partition by order_date)
df.write.mode('overwrite').partitionBy('order_date').parquet(s3_silver)

# Optionally write CSV/Parquet to a prefix that Redshift COPY will use
df.write.mode('overwrite').option('header','true').csv(copy_prefix)

print("Glue transform finished, silver data at", s3_silver)
