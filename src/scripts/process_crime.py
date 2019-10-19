import censusgeocode as cg

from pyspark.sql.functions import udf
from pyspark.sql.functions import col, column
from pyspark.sql.functions import broadcast

from pyspark.sql.types import StringType

from pyspark.sql.functions import from_unixtime, to_timestamp, to_date, unix_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek

from utils import reader
from utils import postgres
from conf import config


def read_crime_data(spark, config, city, year):
    data_src = config.get_crime_data_file(city, year)
    df = reader.read_from_csv(spark, data_src)
    return df


def transform_crime_data(config, city, current_year, df):
    cityname = config.get_city_name(city)
    schema = config.get_crime_data_schema_mapping(city, current_year)
    date_format = config.get_crime_date_format(city, current_year)
    print("Transforming data for %s for the year of %s" % (cityname, current_year))
    df_select = df
    for target, source in schema.items():
        df_select = df_select.withColumnRenamed(source, target)
    df_select = df_select.withColumn('year', year(from_unixtime(unix_timestamp('date', date_format))))
    df_transform = df_select.select(
                ['year', 'date',
                 'category', 'latitude', 'longitude']
            ).where((col("longitude").isNotNull() | col("latitude").isNotNull())
                    ).sort(['longitude', 'latitude'])
    # if current_year != '' and current_year is not None:
    #     df_transform = df_transform.where(col("year") == current_year)
    #     if current_month != '' and current_month is not None:
    #         df_transform = df_transform.where(col("month") == current_month)
    return df_transform


def save_crime_data(city, current_year, df):
    #postgres.save_to_DB(spark, df, "crime_detail", "append")
    dest = "s3a://insight-project/production_crime_detail/%s/%s/" % (city, current_year)
    df.repartition(6).write.parquet(dest, mode='overwrite')


def calculate_crime_data(spark, config, city, df):
    def getTractFromGeo(long, lat):
        try:
            result = cg.coordinates(x=float(long), y=float(lat))
            tract = result['Census Tracts'][0]['GEOID']
        except:
            tract = 'n/a'
        return str(tract)
    udfGetTractFromGeo = udf(getTractFromGeo, StringType())
    lat_long = df.select(['longitude', 'latitude']).dropDuplicates().orderBy(['longitude', 'latitude'])
    geo_tract = lat_long.withColumn("tract", udfGetTractFromGeo("longitude", "latitude"))
    df_crime_geo = df.join(broadcast(geo_tract), ['longitude', 'latitude'], how='left')

# API call tract is slow and join back is slow (30mins each), write to DB might used mappartition

    print("summarizing")
    df_crime_tract_final = df_crime_geo.groupBy(["tract", "year"]).count()
    df_crime_tract_final = df_crime_tract_final.withColumnRenamed("count", "total_count")
    print("writing to DB")
    crime_data_table_name = config.get_crime_data_table_name(city)
    postgres.save_to_DB(spark, df_crime_tract_final, crime_data_table_name, "append")


def process_crime_data(spark, config, city, year_to_process):
    df = read_crime_data(spark, config, city, year_to_process)
    df_transform = transform_crime_data(config, city, year_to_process, df)
    save_crime_data(spark, city, year_to_process, df_transform)
    calculate_crime_data(spark, config, city, df_transform)
    return

