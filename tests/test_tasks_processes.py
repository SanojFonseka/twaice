import pytest
from src._run_scripts_ import *

warnings.filterwarnings('ignore')

@pytest.fixture(scope="session")
def pytest_spark_session(request):

    """
    Create spark session

    Parameter
    ---------
    request : request object
        Provide information on the executing test function.

    Returns
    -------
    SparkSession
        Spark session.
    """

    spark = (
        SparkSession
        .builder
        .master("local[*]")
        .appName("unittesting")
        .config('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
        .config("spark.sql.legacy.timeParserPolicy","LEGACY")
        .config("spark.sql.caseSensitive","true")
        .config("spark.sql.shuffle.partitions", 16)
        .config("spark.default.parallelism", 16)
        .getOrCreate()
        )

    return spark

def df_equality(result, expected_result):

    """
    Check equality of spark datafarmes

    Parameter
    ---------
    df1 : pyspark.sql.dataframe.DataFrame
        spark dataframe.

    df2 : pyspark.sql.dataframe.DataFrame
        spark dataframe.

    Returns
    -------
    boolean
        Boolean state of data frames equality.
    """

    # Check dataframes schemas
    if result.schema != expected_result.schema:
        return False
    
    # Check dataframes values
    if result.collect() != expected_result.collect():
        return False
        
    return True

pytestspark = pytest.mark.usefixtures("pytest_spark_session")

def create_current_sensor_data_for_unit_testing(pytest_spark_session):

    """
    Create spark dataframe for current sensor data for unit testing

    Parameter
    ---------
    pytest_spark_session : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for current sensor data
    current_sensor_data_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("current", DoubleType(), True),
        StructField("container_id", StringType(), True)
        ])

    # Define test data for current sensor data
    current_sensor_data_dict = [
        {"timestamp": 1610582401054000, "current": 0.0, "container_id": "1"},
        {"timestamp": 1610582402650000, "current": 1.0, "container_id": "1"},
        {"timestamp": 1610582404230000, "current": 2.0, "container_id": "1"},
        {"timestamp": 1610582405809000, "current": 2.0, "container_id": "2"},
        {"timestamp": 1610582407393000, "current": -4.0, "container_id": "2"}
    ]

    # Create spark datafarme
    current_sensor_data = pytest_spark_session.createDataFrame(current_sensor_data_dict, current_sensor_data_schema)

    return current_sensor_data

def create_voltage_sensor_data_for_unit_testing(pytest_spark_session):

    """
    Create spark dataframe for voltage sensor data for unit testing

    Parameter
    ---------
    pytest_spark_session : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for voltage sensor data
    voltage_sensor_data_schema = StructType([
        StructField("timestamp", LongType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("container_id", StringType(), True)
        ])

    # Define test data for voltage sensor data
    voltage_sensor_data_dict = [
        {"timestamp": 1610582401054000, "voltage": 1.0, "container_id": "1"},
        {"timestamp": 1610582402650000, "voltage": 1.0, "container_id": "1"},
        {"timestamp": 1610582404230000, "voltage": 1.0, "container_id": "1"},
        {"timestamp": 1610582405809000, "voltage": 1.0, "container_id": "2"},
        {"timestamp": 1610582407393000, "voltage": 1.0, "container_id": "2"}
    ]

    # Create spark datafarme
    voltage_sensor_data = pytest_spark_session.createDataFrame(voltage_sensor_data_dict, voltage_sensor_data_schema)

    return voltage_sensor_data

def create_cumulative_throughput_data_for_unit_testing(pytest_spark_session):

    """
    Create spark dataframe for cumulative throughput data for unit testing

    Parameter
    ---------
    pytest_spark_session : SparkSession
        spark session.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe.
    """

    # Define schema for cumulative throughput data
    cumulative_throughput_schema = StructType([
        StructField("container_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("current", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("throughput", DoubleType(), True),
        StructField("cumulative_throughput", DoubleType(), True)
        ])

    # Define test data for cumulative throughput data
    cumulative_throughput_data_dict = [
        {"container_id": "2", "timestamp": 1610582405809000, "current": 2.0, "voltage": 1.0, "throughput": 2.0, "cumulative_throughput": 2.0},
        {"container_id": "2", "timestamp": 1610582407393000, "current": -4.0, "voltage": 1.0, "throughput": -4.0, "cumulative_throughput": -2.0},
        {"container_id": "1", "timestamp": 1610582401054000, "current": 0.0, "voltage": 1.0, "throughput": 0.0, "cumulative_throughput": 0.0},
        {"container_id": "1", "timestamp": 1610582402650000, "current": 1.0, "voltage": 1.0, "throughput": 1.0, "cumulative_throughput": 1.0},
        {"container_id": "1", "timestamp": 1610582404230000, "current": 2.0, "voltage": 1.0, "throughput": 2.0, "cumulative_throughput": 3.0},
    ]

    # Create spark datafarme
    cumulative_throughput_data = pytest_spark_session.createDataFrame(cumulative_throughput_data_dict, cumulative_throughput_schema)

    return cumulative_throughput_data

def test_calculate_cumulative_throughput(pytest_spark_session):

    """
    Unit testing for calculate cumulative throughput function

    Parameter
    ---------
    pytest_spark_session : SparkSession
        spark session.
    """

    # Define dataframes for unit testoing
    current_data = create_current_sensor_data_for_unit_testing(pytest_spark_session)
    voltage_data = create_voltage_sensor_data_for_unit_testing(pytest_spark_session)
    expected_result = create_cumulative_throughput_data_for_unit_testing(pytest_spark_session)
    
    # Execute CumulativeThroughput function with testing data
    result = CumulativeThroughput("/app/output", "app/output").calculate_cumulative_throughput(current_data,voltage_data)
    
    # Assersion of result and expected results
    assert(df_equality(result, expected_result))