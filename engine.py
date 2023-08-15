# Bring all the components together and run the tracer and span.
# This will allows us to see if the all the components are working together.

import pdb

from app.client.pipeline import PipelineTracer
from app.attributes.spark import SparkObservability

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType 
from pyspark.sql.types import IntegerType

# TODO. include the extraction of the secret from Azure Key Vault
# TODO. if ob account doesn't allow, use the Mesh's

# first, let's create a data sample
spark = SparkSession.builder.appName("spark_test").getOrCreate()

data = [
    ("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([
    StructField("firstname",StringType(),True),
    StructField("middlename",StringType(),True),
    StructField("lastname",StringType(),True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
  ])

df = spark.createDataFrame(data=data, schema=schema)


# second step, let's create a tracer
tracer = PipelineTracer(
    processor_type='BATCH', # default value - illustration purposes
    exporter_type='CONSOLE' # default value - illustration purposes
).get_tracer('engine_test')

# last and third step, let's retrive attributes from the spark Dataframe and session
SparkObservability(spark, tracer, 'local_computer', 'test_session')
SparkObservability(df, tracer, 'local_computer', 'test_data')
