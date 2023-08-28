# Functions that will be used to compute and set the attributes
# of the spark objects that will be monitored.

import json

class get_attributes:
    # TODO. consider more attributes to be included here
    df = lambda df: {
        'columns': df.columns,
        'count': df.count(),
    }
    # TODO. consider more attributes to be included here
    rdd = lambda rdd: {
        'count': rdd.count(),
        'partitions': rdd.getNumPartitions(),
    }
    # TODO. filter some the attributes obtained from the spark session
    ss = lambda ss: {
        conf: val
        for conf, val in ss.sparkContext.getConf().getAll()
    }