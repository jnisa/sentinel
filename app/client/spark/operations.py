# Class that will be used to define the attributes that will be used to monitorize the
# Spark operations

from opentelemetry.sdk.trace import Tracer

from app.client.auxiliars import get_id
from app.client.spark.attributes import get_attributes

class TelescopeSparkOperations:

    def __init__(
        self,
        tracer: Tracer,
        service_id: str
    ):
        
        self._tracer = tracer
        self._service_id = service_id


    @property
    def service_id(self):
        """
        In case the user want to retrieve the service under monitorization.
        """

        return self._service_id

    def df_operation(self, operation_id):
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

        To use this decorator, you just need to add the following line of code to the top of
        your script:

        >>> from app.attributes.spark.operations import TelescopeSparkOperations
        >>> telescope = TelescopeSparkOperations(tracer, service_id)
        >>> @telescope.df_operation('first_last_names_inner_join')
            def my_function(df1, df2):
                # your code here
                return df1.join(df2, df1.id == df2.id, 'inner')

        :param operation_id: The id of the operation that is being monitorized
        :return: The wrapper function that will be used to monitorize the Spark operations
        """

        def decorator(func):
            """
            Decorator function that will be used to monitorize the Dataframe Spark operations.
            """

            # TODO. not sure on the usage of the kwargs here
            def wrapper(*args, **kwargs):

                with self._tracer.start_as_current_span(name=get_id(self._service_id, operation_id)) as span:

                    # observability over the arguments provided                    
                    attributes = [
                        {f'df{df_idx + 1}': str(get_attributes.df(df))}
                        for df_idx, df in enumerate(args)
                    ]

                    # observability over the result of the function
                    result = func(*args, **kwargs)
                    attributes.append({'df_result': str(get_attributes.df(result))})

                    # add the attributes to the span
                    for att in attributes:
                        span.set_attributes(att)

                return result
            return wrapper
        return decorator

    def rdd_operation(self, operation_id):
        """
        RDDs are the building blocks of Spark. They are immutable distributed collections of objects.
        The operations that can be performed on RDDs can be classified into two categories:
        - Transformations: Transformations are operations on RDDs that return a new RDD.
        - Actions: Actions are operations that return a result to the driver program or write it to
        storage.

        This decorator will be used to monitorize some of these operations to a pyspark RDD.
        Throughout the usage of this decorator, we will be able to monitorize the following attributes
        of the RDDs received as parameters:
        - name of the RDD
        - number of partitions
        - number of records
        
        To use this decorator, you just need to add the following line of code to the top of
        your script:

        >>> from app.attributes.spark.operations import TelescopeSparkOperations
        >>> telescope = TelescopeSparkOperations(tracer, service_id)
        >>> @telescope.rdd_operation('rdd_test_filter')
            def is_even(rdd):
                # your code here
                return rdd.filter(lambda x: x % 2 == 0)

        :param operation_id: The id of the operation that is being monitorized
        :return: The wrapper function that will be used to monitorize the Spark operations
        """

        def decorator(func):
            """
            Decorator function that will be used to monitorize the Spark RDD operations.
            """

            # TODO. not sure on the usage of the kwargs here
            def wrapper(*args, **kwargs):

                with self._tracer.start_as_current_span(name=get_id(self._service_id, operation_id)) as span:

                    # observability over the arguments provided                    
                    attributes = [
                        {f'rdd{rdd_idx + 1}': str(get_attributes.rdd(rdd))} 
                        for rdd_idx, rdd in enumerate(args)
                    ]

                    # observability over the result of the function
                    result = func(*args, **kwargs)
                    attributes.append({'rdd_result': str(get_attributes.rdd(result))})

                    # add the attributes to the span
                    for att in attributes:
                        span.set_attributes(att)

                return result
            return wrapper
        return decorator
