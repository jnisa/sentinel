# Bring all the components together and run the tracer and span.
# This will allows us to see if the all the components are working together.

from app.client.pipeline import PipelineTracer
from app.client.spark.resources import TelescopeSparkResources 
from app.client.spark.operations import TelescopeSparkOperations

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType 
from pyspark.sql.types import IntegerType

# first, let's create a data sample
spark = SparkSession.builder.appName("spark_test").getOrCreate()

data1 = [
  ("James","","Smith","36636","M",3000),
  ("Michael","Rose","","40288","M",4000),
  ("Robert","","Williams","42114","M",4000),
  ("Maria","Anne","Jones","39192","F",4000),
  ("Jen","Mary","Brown","","F",-1)
]
data2 = [
  ("36636","London"),
  ("40288","Manchester"),
  ("42114","Birmingham"),
  ("39192","New York")
]

schema1 = StructType([
  StructField("firstname",StringType(),True),
  StructField("middlename",StringType(),True),
  StructField("lastname",StringType(),True),
  StructField("id", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("salary", IntegerType(), True)
])
schema2 = StructType([
  StructField("id",StringType(),True),
  StructField("location",StringType(),True)
])

df1 = spark.createDataFrame(data=data1, schema=schema1)
df2 = spark.createDataFrame(data=data2, schema=schema2)

# second step, let's create a tracer
tracer = PipelineTracer(
    processor_type='BATCH', # default value - illustration purposes
    exporter_type='CONSOLE' # default value - illustration purposes
).get_tracer('engine_test')

# last and third step, let's retrive attributes from the spark Dataframe and session
# TelescopeSparkResources(tracer, 'local_computer', spark, 'test_session')
# TelescopeSparkResources(tracer, 'local_computer', df1, 'test_data')

# initialize the telescope for spark operations
telescope = TelescopeSparkOperations(tracer, 'local_computer')

# define an function to join both dataframes
@telescope.df_operation('test_inner_join')
def inner_join_test(df1, df2):
    return df1.join(df2, df1.id == df2.id, 'inner')

# call the function
inner_join_test(df1, df2)
