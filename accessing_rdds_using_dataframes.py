from pyspark import SparkContext
from pyspark.sql import Row
from pyspark import sql

sc = SparkContext()
sql_context = sql.SQLContext(sc)


def create_rdd(list):
    return sc.parallelize(list)


def convert_rdd_to_dataframe(rdd):
    return rdd.toDF()


if __name__ == "__main__":
    complex_dataframe = convert_rdd_to_dataframe(create_rdd([Row(col_float=1.23,
                                                                 col_int=1,
                                                                 col_string="gg",
                                                                 col_list=[1, 2, 3, 4],
                                                                 col_row=Row(place="bfahfi",
                                                                             mobileNo=69689689)),
                                                             Row(col_float=2.23,
                                                                 col_int=2,
                                                                 col_string="dhr",
                                                                 col_list=[5, 6, 1, 58],
                                                                 col_row=Row(place="wtwet", mobileNo=534646)),
                                                             ]))
    complex_dataframe.show()