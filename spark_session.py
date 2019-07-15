from pyspark.sql import SparkSession

spark_session = SparkSession.builder.appName('Analysing London Crimes').getOrCreate()


def read_data(filename):
    return  spark_session.read.format("csv").option("header","true").load(filename)

if __name__ == "__main__":
    data = read_data(r'C:\Users\achakala.sreenath\PycharmProjects\spark2\demo\datasets\london_crime_by_lsoa.csv')