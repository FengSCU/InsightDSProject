import os

dbUrl = os.environ['POSTGRES_URL']
user = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASSWORD']
properties={"user": user, "password": password}
dbTableMortgageHistory = "mortgage_history"
dbTableIncomeLevel = "income_level"
dbTableCrime = "crime"
mode = "overwrite"


def save_to_DB(spark, df, table_name, mode):
    if mode == 'append' and not table_exists_in_DB(spark, table_name):
        mode = 'overwrite'
    result = df.write \
        .mode(mode) \
        .jdbc(dbUrl,
              table_name,
              properties=properties)
    return result


def read_from_DB(spark, table_name):
    jdbcDF = spark.read \
        .jdbc(dbUrl,
              table_name,
              properties=properties)
    return jdbcDF


def table_exists_in_DB(spark, table_name):
    query = "SELECT EXISTS (SELECT 1 FROM   pg_tables WHERE  tablename = '%s')" % table_name
    df = spark.read.format("jdbc")\
        .option("url", dbUrl)\
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .load()
    try:
        result = df.collect()[0][0]
    except Exception:
        result = False
    return result


def get_county_for_city_from_DB(spark, city, state):
    query = "SELECT countyname, countycode from city_to_county where cityname like '%s%%' and state='%s'" % (city, state)
    df = spark.read.format("jdbc")\
        .option("url", dbUrl)\
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .load()
    try:
        county_name = df.collect()[0][0][:-3]
        county_code = df.collect()[0][1]
    except Exception:
        county_name = ""
        county_code = ""
    return county_name, county_code
