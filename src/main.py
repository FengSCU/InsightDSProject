from scripts import process_mortgage
from scripts import process_crime
from scripts import process_school
from conf import config

from pyspark.sql import SparkSession


def create_spark_session():
    """
    This function is used to create spark session, with that we can run the spark
    """
    spark = SparkSession \
        .builder \
        .appName('DataProcessor') \
        .getOrCreate()
    return spark


def main():
    spark = create_spark_session()
    conf = config.Config()

    city_list = conf.get_city_list()

    year_to_process = 2018

    print("Processing Mortgage Data")
    df = process_mortgage.read_mortgage_data(spark, year_to_process)
    for city in city_list:
        process_mortgage.process_mortgage_data(spark, conf, city, year_to_process, df)

    print("Processing Crime Data")
    for city in city_list:
        process_crime.process_crime_data(spark, conf, city, year_to_process)

    print("Processing School Data")
    for city in city_list:
        process_school.process_school_data(spark, conf, city)
    return


if __name__ == "__main__":
    main()


