from scripts import process_mortgage
from scripts import process_crime
from scripts import process_school
from conf import config
from datetime import datetime

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

    today = datetime.today()
    year_to_process = today.year
    month_to_process = today.month

    # mortgage data is updated annually
    if month_to_process == 1:
        print("Processing Mortgage Data")
        df = process_mortgage.read_mortgage_data(spark, conf, year_to_process)
        for city in city_list:
            process_mortgage.process_mortgage_data(spark, conf, city, year_to_process, df)

    # mortgage data is updated monthly
    print("Processing Crime Data")
    for city in city_list:
        process_crime.process_crime_data(spark, conf, city, year_to_process, month_to_process)

    # get latest school data
    print("Processing School Data")
    for city in city_list:
        process_school.process_school_data(spark, conf, city)
    return


if __name__ == "__main__":
    main()


