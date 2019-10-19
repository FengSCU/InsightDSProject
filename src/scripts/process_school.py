from utils import reader
from utils import postgres
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

import censusgeocode as cg

def read_school_data(spark, config, city):
    data_src = config.get_school_data_file(city)
    df = reader.read_from_xml(spark, data_src, "schools", "school")
    return df


def process_school_data(spark, config, city):
    """
    This function is used to load school datasets from S3, use it to create tables.
    spark has package to SPARK-XML to process
    """
    # read file
    df_school = read_school_data(spark, config, city)

    df_school_select = df_school.select(['name', 'enrollment', 'gradeRange', 'gsRating', 'lat', 'lon'])

    # df_school_select.write.parquet("s3a://insight-project/school-sf", mode='overwrite')

    def getTractFromGeo(long, lat):
        try:
            result = cg.coordinates(x=long, y=lat)
            tract = result['Census Tracts'][0]['GEOID']
        except:
            tract = 'n/a'
        return str(tract)

    udfGetTractFromGeo = udf(getTractFromGeo, StringType())

    df_school_select_w_tract = df_school_select.withColumn("tract", udfGetTractFromGeo("lon", "lat"))

    postgres.save_to_DB(df_school_select_w_tract, "school", "overrite")

    return
