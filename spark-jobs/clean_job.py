from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from datetime import datetime, timedelta
from pyspark.sql.types import TimestampType, DateType


def create_spark_session():
    """
    Creates a spark session if not exists.

    Returns:
        (obj) spark - a spark session
    """
    spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_input_data(spark, input_source, output_data):
    """
    Fetch the input data from input source.
    Cleans the data and fix all data quality issues
    Then the data is pushed to Amazon S3.

    Args:
        (obj) spark - spark session object to handle spark processes
        (str) input_source - The input source to fetch data from
        (str) output_data - The output location to push data
        
    """
    # Read SAS format input from S3 bucket
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(input_source)

    # Create fiels that are required to map and process
    required_fields = ['cicid', 'arrdate', 'depdate', 'i94res', 'i94bir', 'gender', 'i94visa', 'i94mode', 'airline']
    new_fields = ['id', 'arrival_date', 'departure_date', 'arrival_country', 'age', 'gender', 'visa_type', 'mode',
                  'airline']

    for i in range(len(required_fields)):
        required_fields[i] = required_fields[i] + ' as ' + new_fields[i]

    df_spark = df_spark.selectExpr(*required_fields)

    # Drop rows of data from dataframe in which any field except airline is null,
    # as airline can be null if mode is different than air
    df_spark = df_spark.dropna(
        subset=['arrival_date', 'departure_date', 'arrival_country', 'age', 'gender', 'mode', 'visa_type'], how='any')

    # Drop rows in which mode is other than air and contains airline name
    df_spark = df_spark.where(((df_spark.mode == 1) & (df_spark.airline.isNotNull())) | (
            (df_spark.mode.isin(2, 3, 9)) & (df_spark.airline.isNull())))

    # Calculate the no. of days people spend in US
    df_spark = df_spark.withColumn('duration', df_spark.departure_date - df_spark.arrival_date)

    timestamps = udf(lambda x: datetime(1960, 1, 1) + timedelta(days=x), DateType())

    # Transform date format from SAS format to readable iso8601 format
    df_spark = df_spark.withColumn('arrival_date', timestamps(df_spark.arrival_date)) \
        .withColumn('departure_date', timestamps(df_spark.departure_date))

    # Drop rows of data in which departure date is less than arrival date
    df_spark = df_spark.where(df_spark.duration >= 0)

    # Transform all the double values to int
    df_spark = df_spark.withColumn('id', col('id').cast('int')) \
        .withColumn('arrival_country', col('arrival_country').cast('int')) \
        .withColumn('age', col('age').cast('int')) \
        .withColumn('visa_type', col('visa_type').cast('int')) \
        .withColumn('mode', col('mode').cast('int')) \
        .withColumn('duration', col('duration').cast('int'))

    # Save the cleaned data to Amazon S3
    df_spark.write.csv(output_data)


def main():
    """
    Start of the spark job
    Takes data from input, process the data and then save the data to output path
    """
    spark = create_spark_session()
    input_source = "s3://capstone-demo-project/immigration-data/i94_apr16_sub.sas7bdat"
    output_data = "s3://capstone-demo-project/immigration-data/cleanjob.csv"

    process_input_data(spark, input_source, output_data)


if __name__ == "__main__":
    main()
