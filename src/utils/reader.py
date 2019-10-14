def read_from_csv(spark, filepath):
    """
    source file has header
    """
    df = spark.read.format("csv"
                           ).option("header", "true").option("inferSchema", "true").load(filepath)
    return df


def read_from_csv_with_schema(spark, filepath, schema):
    """
    source file does not have header
    """
    df = spark.read.csv(filepath, header=False, schema=schema)
    return df


def read_from_xml(spark, filepath, rootTag, rowTag):
    df = spark.read.format('com.databricks.spark.xml'
                                  ).option('rootTag', rootTag).option('rowTag', rowTag).load(filepath)
    return df


def read_from_txt(spark, filepath, delimiter="|"):
    """
    source file has header
    """
    df = spark.read.format("csv").option("header", "true"
                                         ).option("inferSchema", "true"
                                                  ).option("delimiter", delimiter).load(filepath)
    return df

