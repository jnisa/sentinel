# Class that will be used to define the attributes that will be used to monitorize the
# Spark operations

from opentelemetry.sdk.trace import Tracer

from pyspark.sql import DataFrame
from pyspark.rdd import RDD

from app.client.service import ServiceSpan
from app.attributes.auxiliars import get_id


class TelescopeSparkOperations:

    def __init__(
        self,
        tracer: Tracer,
        service_id: str,
        operation_id: str
    ):
        
        self._tracer = tracer
        self._service_id = service_id
        self._operation_id = operation_id


    @property
    def service_id(self):
        """
        In case the user want to retrieve the service under monitorization.
        """

        return self._service_id

    @property
    def operation_id(self):
        """
        In case the user want to retrieve the identifier of the operations that will be performed.
        """

        return self._operation_id

    def df_operation(self, value):
        """
        A considerable amount of operations can be performed on a pyspark dataframe. Some of the
        most common operations are:
        - join
            >>> df1.join(df2, df1.id == df2.id, 'inner') # inner join
        - union
            >>> df1.union(df2) # union of two dataframes
        - withColumn
            >>> merged_df.withColumn('Salary', merged_df['df2.Salary']) # to rename a column

        This decorator will be used to monitorize some of these operations to a pyspark dataframe.
        Throughout the usage of this decorator, we will be able to monitorize the following attributes
        of the dataframes received as parameters:
        - columns
        - number of records
        - dtypes
        - schema

        To use this decorator, you just need to add the following line of code to the top of
        your script:

        >>> from app.attributes.spark.operations import TelescopeSparkOperations
        >>> telescope = TelescopeSparkOperations(tracer, service_id, operation_id)
        >>> @telescope.df_operation
            def my_function(df1, df2):
                # your code here
                return df1.join(df2, df1.id == df2.id, 'inner')

        :return: The wrapper function that will be used to monitorize the Spark operations
        """

        # TODO. not sure on the usage of the kwargs here
        def decorator(func):
            """
            Decorator function that will be used to monitorize the Spark operations.
            """

            # TODO. not sure on the usage of the kwargs here
            def wrapper(*args, **kwargs):

                # TODO. consider more attributes to be included here     
                # TODO. consider the reallocation of this method           
                df_attributes = lambda df: {
                        'columns': df.columns,
                        'count': df.count(),
                        'dtypes': df.dtypes,
                        'schema': df.schema,
                    }

                if all(isinstance(df, DataFrame) for df in args):

                    # observability over the arguments provided                    
                    attributes = {
                        f'df_{df_idx}': df_attributes(df)
                        for df_idx, df in enumerate(args)
                    }

                    # observability over the result of the function
                    result = func(*args, **kwargs)
                    attributes.update({'df_result': df_attributes(result)})

                    # add the attributes to the span
                    for k, v in attributes.items():
                        span_id = get_id(self.service_id, self.operation_id, k)
                        with self._tracer.start_as_current_span(name=span_id) as span:
                            span.set_attributes(v)

                else:
                    raise Exception('The arguments of the function must be dataframes.')

                return result
            return wrapper
        return decorator
