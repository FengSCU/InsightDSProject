import censusgeocode as cg

from pyspark.sql.functions import udf
from pyspark.sql.functions import col, column

from pyspark.sql.types import StringType

from pyspark.sql.functions import from_unixtime, to_timestamp, to_date, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek

from src.utils import reader
from src.utils import postgres


def read_crime_data(spark, config, city, year):
    data_src = config.get_crime_data_file(city, year)
    df = reader.read_from_csv(spark, data_src)
    return df


def transform_crime_data(current_year, current_month, df):
    if current_year <= 2017:
        print("Transforming data pre 2018")
        df_select = df.withColumnRenamed("X", "longitude")
        df_select = df_select.withColumnRenamed("Y", "latitude")
        df_select = df_select.withColumn('year', year(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))
        df_select = df_select.withColumn('month', month(from_unixtime(unix_timestamp('Date', 'MM/dd/yyyy'))))
        df_select = df_select.select(
            ['year', 'month', 'Date', 'Time', 'DayOfWeek',
             'Category', 'latitude', 'longitude']
        ).where((col("longitude").isNotNull() | col("latitude").isNotNull())
                ).sort(['longitude', 'latitude'])
        df_transform = df_select.withColumnRenamed("Date", "date")
        df_transform = df_transform.withColumnRenamed("Time", "time")
        df_transform = df_transform.withColumnRenamed("DayofWeek", "dayofweek")
        df_transform = df_transform.withColumnRenamed("Category", "category")
    elif current_year >= 2018:
        print("Transforming data post 2018")
        df_select = df.select(
            ['Incident Year', 'Incident Date', 'Incident Time', 'Incident Day of Week',
             'Incident Category', 'Latitude', 'Longitude']
            ).where(col("Latitude").isNotNull() | col("Longitude").isNotNull()
                    ).sort(['Longitude', 'Latitude'])
        # rename column names to remove whitespace
        df_transform = df_select.withColumnRenamed("Incident Year", "year")
        df_transform = df_transform.withColumn("month", month(from_unixtime(unix_timestamp("Incident Date", 'yyyy/MM/dd'))))
        df_transform = df_transform.withColumnRenamed("Incident Date", "date")
        df_transform = df_transform.withColumnRenamed("Incident Time", "time")
        df_transform = df_transform.withColumnRenamed("Incident Day of Week", "dayofweek")
        df_transform = df_transform.withColumnRenamed("Incident Category", "category")
        df_transform = df_transform.withColumnRenamed("Longitude", "longitude")
        df_transform = df_transform.withColumnRenamed("Latitude", "latitude")
    df_transform = df_transform.where((col("year") == current_year) & (col("month") == current_month))
    return df_transform


def save_crime_data(spark, city, current_year, current_month, df):
    postgres.save_to_DB(spark, df, "crime_detail", "append")
    dest = "s3a://insight-project/production_crime_detail/%s/%s/%s/" % (city, current_year, current_month)
    df.repartition(6).write.parquet(dest, mode='overwrite')


def calculate_crime_data(spark, df):
    def getTractFromGeo(long, lat):
        try:
            result = cg.coordinates(x=long, y=lat)
            tract = result['Census Tracts'][0]['GEOID']
        except KeyError or IndexError:
            tract = 'n/a'
        return str(tract)
    udfGetTractFromGeo = udf(getTractFromGeo, StringType())
    lat_long = df.select(['Longitude','Latitude']).dropDuplicates()
    geo_tract = lat_long.withColumn("tract", udfGetTractFromGeo("longitude", "latitude"))
    df_crime_geo = geo_tract.join(df, ['Longitude', 'Latitude'], how='right')
    # TODO geo_tract [Postgres:neighbor_detail]
    print("summarizing")
    df_crime_tract_final = df_crime_geo.groupBy(["tract", "year", "month"]).count()
    df_crime_tract_final = df_crime_tract_final.withColumnRenamed("count", "sum(count)")
    print("writing to DB")
    postgres.save_to_DB(spark, df_crime_tract_final, "production_crime_data", "append")


def process_crime_data(spark, config, city, year_to_process, month_to_process):
    cityname = config.get_city_name(city)
    df = read_crime_data(spark, config, city, year_to_process)
    df_transform = transform_crime_data(year_to_process, month_to_process, df)
    save_crime_data(spark, cityname, year_to_process, month_to_process, df_transform)
    calculate_crime_data(spark, df_transform)
    return

