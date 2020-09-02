from pyspark.sql import SparkSession
from pyspark.sql.functions import when, dayofweek, col, date_format
import sys

def create_spark_session():
    """
    Creates a spark session if not exists.

    Returns:
        (obj) spark - a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.es.nodes.wan.only", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_input_data(spark, input_source, output_data, username, password):
    """
    Fetch the input data from Redshift tables using jdbc drivers.
    Then joins the data of all dataframes and perform tarnsformation on that.
    Then the data is pushed to elastic cluster.

    Args:
        (obj) spark - spark session object to handle spark processes
        (str) input_source - The input source to fetch data from
        (str) output_data - The output location to push data
        (str) username - username of database
        (str) password - password of database
        
    """

    # Read data from happiness dimension table from Amazon Redshift
    df_country = spark.read \
        .format("jdbc") \
        .option("url", input_source) \
        .option("dbtable", "happiness_dim") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Read data from mode dimension table from Amazon Redshift
    df_mode = spark.read \
        .format("jdbc") \
        .option("url", input_source) \
        .option("dbtable", "mode_dim") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Read data from visa dimension table from Amazon Redshift
    df_visa = spark.read \
        .format("jdbc") \
        .option("url", input_source) \
        .option("dbtable", "visa_dim") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Read data from immigration fact table from Amazon Redshift
    df_spark = spark.read \
        .format("jdbc") \
        .option("url", input_source) \
        .option("dbtable", "immigration_fact") \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Join all the dataframes 
    df_final = df_spark.join(df_visa, df_spark.visa_type == df_visa.id)\
                       .join(df_mode, df_spark.mode == df_mode.id) \
                       .join(df_country, df_spark.arrival_country == df_country.id) \
                       .select(
                            [df_spark.id,
                             'arrival_date',
                             'departure_date',
                             'age',
                             'gender',
                             'visa',
                             df_mode.mode,
                             'airline',
                             'duration',
                             'country',
                             'region',
                             'happiness_score',
                             'economy'
                             ]
    )



    df_final = df_final.na.fill('NA')

    # Extract happiness_score, economy score of united states for comparison
    x = df_country.select(['country', 'happiness_score', 'economy']).where(df_country.country == 'United States')

    h_score = x.head()[1]
    e_score = x.head()[2]

    # Create new columns based on comparison of happiness score and economy score
    df_final = df_final\
        .withColumn('is_more_happy', when(df_final.happiness_score <= h_score, 'False').otherwise('True')) \
        .withColumn('is_better_economy', when(df_final.economy <= e_score, 'False').otherwise('True')) \
        .withColumn('day', dayofweek(df_final.arrival_date))

    # Transform date format
    df_final = df_final.withColumn('happiness_score', col('happiness_score').cast('double')) \
        .withColumn('economy', col('economy').cast('double')) \
        .withColumn("arrival_date", date_format(col("arrival_date"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("departure_date", date_format(col("departure_date"), "yyyy-MM-dd'T'HH:mm:ss"))

    # Push data to elastic
    df_final.write.format('org.elasticsearch.spark.sql') \
        .option('es.nodes', output_data) \
        .option('es.port', 9200) \
        .option('es.resource', '%s/%s' % ('immigration-', 'immigration')) \
        .mode('append') \
        .save()

def main():
    """
    Start of the spark job
    Takes data from Redshift tables, process the data and then push the data to elastic.
    """
    spark = create_spark_session()
    input_source = "jdbc:postgresql://xxxxx.xxxxxxxx.us-west-2.redshift.amazonaws.com:5439/dev"
    username = 'awsuser'
    password = 'xxxxx'
    output_data = sys.argv[1].strip()

    process_input_data(spark, input_source, output_data, username, password)


if __name__ == "__main__":
    main()
