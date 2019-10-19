from pyspark.sql.functions import udf
from pyspark.sql.functions import col

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from utils import reader
from utils import postgres

schema = StructType([
    StructField("activity_year", IntegerType(), True),
    StructField("loan_amount", DoubleType(), True),
    StructField("census_tract", StringType(), True),
    StructField("income", StringType(), True),
    StructField("income_int", IntegerType(), True),
    StructField("level", IntegerType(), True)])


def read_mortgage_data(spark, config, year):
    data_src = config.get_mortgage_data_file(year)
    if int(year) <= 2017:
        df = reader.read_from_csv(spark, data_src)
        df_filtered = df.filter(
            (df.action_taken == 1) & (
                    df.loan_purpose == 1) & (df.property_type == 1))
    else:
        df = reader.read_from_txt(spark, data_src)
        df_filtered = df.filter(
            (df.action_taken == 1) & (
                    df.loan_purpose == 1) & (df.derived_dwelling_category.like('Single Family%')))
    return df_filtered


def get_data_for_city(config, year, city, df):
    county_name = config.get_county_name_for_city(city)
    county_code = config.get_county_code_for_city(city)

    if year <= 2017:
        df_city = df.filter(df.county_name == county_name)
    else:
        df_city = df.filter(df.county_code == county_code)
    return df_city


def transform_mortgage_data(config, year, city, df):
    county_code = config.get_county_code_for_city(city)
    data_schema = config.get_mortgage_data_schema_mapping(year)
    df_select = df
    for target, source in data_schema.items():
        df_select = df_select.withColumnRenamed(source, target)

    def transform_tract_num(tract_num):
        """Change the tract format that same as 2018 tract"""
        if tract_num is None or len(tract_num) == 11:
            tract = tract_num
        else:
            tract = county_code + str(tract_num).replace('.', '')
        return tract
    udf_transform_tract_num = udf(transform_tract_num, StringType())
    df_transform = df_select.withColumn('census_tract', udf_transform_tract_num('census_tract'))
    df_transform = df_transform.where(col("activity_year") == year)
    return df_transform


def save_mortgage_data_for_city(spark, city, year, df):
    # save_to_DB(spark, df, "mortgage_detail", "append")
    dest = "s3a://insight-project/production_mortgage_detail/%s/%s/" % (city, year)
    df.write.parquet(dest, mode='overwrite')


def extract_tract_info_from_mortgage_data(spark, config, df):
    df_tract = df.select(['activity_year', 'census_tract', 'tract_population',
                          'tract_owner_occupied_units', 'tract_one_to_four_family_homes']).dropDuplicates()
    table_name = config.get_mortgage_tract_table_name()

    postgres.save_to_DB(spark, df_tract, table_name, 'append')
    return


def calculate_mortgage_tract(spark, year, df):
    """Unifying mortgage data and aggregate by tract"""
    df_mortgage = df.select(['activity_year', 'loan_amount', 'census_tract', 'income'])

    df_mortgage_w_income = df_mortgage\
        .withColumn('income_int', col('income').cast("int"))\
        .withColumn('activity_year', col('activity_year').cast("int")) \
        .where((col('income_int') != 0) | (col('income_int').isNotNull()))
    # df_mortgage_w_income = df_mortgage_w_income.where((mortgage_sf_update.income_int != 0) | (col('income_int').isNotNull()))
    # process income type
    def income_level(income):
        """
        0-100: 1
        460+: 20
        """
        level = 25
        if income < 500:
            level = income / 20 + 1
        return level
    udflevel = udf(income_level, IntegerType())
    # TODO sf_level [Postgres:neighbor_detail]
    df_mortgage_w_level = df_mortgage_w_income.withColumn('level', udflevel('income_int'))
    postgres.save_to_DB(spark, df_mortgage_w_level, 'production_mortgage_data', 'append')
    # df_mortgage_count = df_mortgage_w_level.filter(df_mortgage_w_level.activity_year == year).count()
    df_mortgage_aggr = df_mortgage_w_level.filter(df_mortgage_w_level.activity_year == year).select(
        ['activity_year', 'level', 'census_tract']).groupBy(['activity_year', 'level', 'census_tract']).count()
    df_mortgage_aggr.createTempView('df_mortgage_aggr')
    df_mortgage_rank = spark.sql(
    """SELECT *, RANK()OVER(PARTITION BY level ORDER BY count DESC) AS rank
       FROM df_mortgage_aggr
    """)
    # TODO ensure rankN in [Postgres:neighbor_detail]
    postgres.save_to_DB(spark, df_mortgage_rank, "production_mortgage_rankn", "append")
    return df_mortgage_rank


def process_mortgage_data(spark, config, city, year_to_process, df):
    #df = read_mortgage_data(spark, config, year_to_process)
    df_city = get_data_for_city(config, year_to_process, city, df)
    # process data
    df_transform = transform_mortgage_data(config, year_to_process, city, df_city)
    save_mortgage_data_for_city(spark, city, year_to_process, df_transform)
    extract_tract_info_from_mortgage_data(spark, config, df_transform)
    df_mortgage_rank = calculate_mortgage_tract(spark, year_to_process, df_transform)
    return df_mortgage_rank
