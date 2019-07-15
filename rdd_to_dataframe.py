from pyspark import SparkContext
from pyspark.sql import Row
from pyspark import sql
from datetime import datetime

sc = SparkContext()
sql_context = sql.SQLContext(sc)


def create_rdd(list):
    return sc.parallelize(list)


def convert_rdd_to_dataframe(rdd):
    return rdd.toDF()


def create_dataframe_using_sqlcontext(data, column_names):
    return sql_context.createDataFrame(data, column_names)


def create_dataframe_using_sqlcontext_with_rdd(rdd):
    return sql_context.createDataFrame(rdd)


if __name__ == "__main__":
    simple_dataframe = convert_rdd_to_dataframe(create_rdd([Row(id=1, name="sreenath", age="22"),
                                                            Row(id=2, name="sreekanth", age="26")]))
    complex_dataframe = convert_rdd_to_dataframe(create_rdd([Row(col_float=1.23,
                                                                 col_int=1,
                                                                 col_string="gg"),
                                                             Row(col_float=2.23,
                                                                 col_int=2,
                                                                 col_string="dhr")]))
    data_frame_with_list = convert_rdd_to_dataframe(create_rdd([Row(col_float=1.23,
                                                                    col_int=1,
                                                                    col_string="gg",
                                                                    col_list=[1, 2, 3, 4]),
                                                                Row(col_float=2.23,
                                                                    col_int=2,
                                                                    col_string="dhr",
                                                                    col_list=[5, 6, 1, 58]),
                                                                ]))
    data_frame_with_nested_rows = convert_rdd_to_dataframe(create_rdd([Row(col_float=1.23,
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
    data_frame_using_sql_context = create_dataframe_using_sqlcontext([(1,
                                                                       "guyg",
                                                                       2.3,
                                                                       Row(1, 2, 3),
                                                                       [1, 2, 3])],
                                                                     ['1', '2', '3', '4', '5'])
    data_frame_using_sql_context_with_rdd = create_dataframe_using_sqlcontext_with_rdd(
        create_rdd([Row(id=1, name="sreenath", age="22"),
                    Row(id=2, name="sreekanth", age="26")]))

    simple_dataframe.show()
    complex_dataframe.show()
    data_frame_with_list.show()
    data_frame_with_nested_rows.show()
    data_frame_using_sql_context.show()
    data_frame_using_sql_context_with_rdd.show()
