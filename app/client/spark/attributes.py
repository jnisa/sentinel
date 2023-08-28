# Functions that will be used to compute and set the attributes
# of the spark objects that will be monitored.

class get_attributes:
    # TODO. consider more attributes to be included here
    df = lambda df_id, df: {
        'df': df_id,
        'columns': df.columns,
        'count': df.count(),
        'dtypes': df.dtypes,
    }
    # TODO. consider more attributes to be included here
    rdd = lambda rdd_id, rdd: {
        'rdd': rdd_id,
        'count': rdd.count(),
        'partitions': rdd.getNumPartitions()
    }
    # TODO. filter some the attributes obtained from the spark session
    ss = lambda ss: {
        conf: val
        for conf, val in ss.sparkContext.getConf().getAll()
    }