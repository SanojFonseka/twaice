from src.utilities import *

class SensorDataEnrichment:
  
  """
    Enrich sensor data with metadata and write each sensor type's data to persistant storage as parquet tables

    Parameter
    ---------
    input_directory : generic file path to input data
        Sensor raw data.
        
    output_directory : generic file path for output data
        Metadata.

  """
  
  def __init__(self, input_directory, output_directory):
    self.spark = (SparkSession.builder
                  .master("local[*]")
                  .appName("SensorDataEnrichment")
                  .config('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
                  .config("spark.sql.legacy.timeParserPolicy","LEGACY")
                  .config("spark.sql.caseSensitive","true")
                  .config("spark.sql.shuffle.partitions", 16)
                  .config("spark.default.parallelism", 16)
                  .getOrCreate())
    

    self.input_directory = input_directory
    self.output_directory = output_directory
    
    self.logger = logging.getLogger("SensorDataEnrichment")

    # Define logg pattern
    self.formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)',datefmt='%Y-%m-%d %H:%M:%S')
    self.logger.setLevel(logging.INFO)
    self.console = logging.StreamHandler()
    self.console.setFormatter(self.formatter)
    self.logger.addHandler(self.console)
        
  def extract_sensor_data(self):
    
    """
    Load sensor data from parquet tables

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains sensor raw data.
    """

    try:
      self.logger.info("start reading sensor data")
      
      sensor_data_schema = StructType([
        StructField("tid", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("value", DoubleType(), True)
        ])

      # read sensor data from parqute table from the input directory
      sensor_data = (self.spark.read.format("parquet")
                     .schema(sensor_data_schema)
                     .load(f"{self.input_directory}/challenge_data.parquet.gzip")
                     )
      
      self.logger.info("end reading sensor data")
      return sensor_data
  
    except Exception:
      self.logger.error(msg = "falied reading sensor data", exc_info = True)
  
  def extract_metadata(self):
    
    """
    Load metadata from csv files

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains metadata.
    """
    
    try:
      self.logger.info("start reading metadata")
                       
      metadata_schema = StructType([
        StructField("container_id", StringType(), False),
        StructField("tid", StringType(), False),
        StructField("sensor_name", StringType(), False),
        StructField("sensor_type", StringType(), False)
        ])

      # read metadata from csv file from the input directory
      metadata = (self.spark.read.option("header",True)
                  .schema(metadata_schema)
                  .csv(f"{self.input_directory}/challenge_metadata.csv")
                  )
      
      self.logger.info("end reading metadata")
      return metadata
  
    except Exception:
      self.logger.error(msg = "falied reading metadata", exc_info = True)
  
  def enrichment(self, sensor_data, metadata):
    
    """
    Enrich sensor raw data with metadata

    Parameter
    ---------
    sensor_data : pyspark.sql.dataframe.DataFrame
        Sensor raw data.
        
    metadata : pyspark.sql.dataframe.DataFrame
        Metadata.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains enriched sensor data.
    """
    
    try:
      self.logger.info("start enriching sensor data")
                       
      # join sensor data and metadata based tid for enrichment purposes
      enriched_data = (sensor_data.join(metadata, on='tid', how='inner')
                       .withColumn("updated_timestamp", f.current_timestamp())
                       .withColumn("date", f.to_date((f.col("timestamp")/1000000).cast('timestamp')))
                       )
      
      self.logger.info("end enriching sensor data")
      return enriched_data
  
    except Exception:
      self.logger.error(msg = "falied enriching sensor data", exc_info = True)
  
  def persist_data(self, enriched_data):
    
    """
    Write enriched data to persistant storage based on sesnsor type

    Parameter
    ---------
    enriched_data : pyspark.sql.dataframe.DataFrame
        Enriched sensor data.

    Returns
    -------
    parquet table
        Persist data to stotrage using parquet format.
    """
    
    try:
      self.logger.info("start writing current sensor data")

      # filter current sensor data from the enriched sensor data
      enriched_current_data = enriched_data.filter(f.col("sensor_type") == "Current")

      # write current sensor data to parquet table in output directory
      (enriched_current_data.write.format("parquet")
       .option("mergeSchema","true")
       .mode("overwrite")
       .partitionBy("date","container_id")
       .save(f"{self.output_directory}/enriched_current_data")
       )

      self.logger.info("end writing current sensor data")
    
    except Exception:
      self.logger.error(msg = "falied writing current sensor data", exc_info = True)
    
    try:
      self.logger.info("start writing voltage sensor data")

      # filter voltage sensor data from the enriched sensor data
      enriched_voltage_data = enriched_data.filter(f.col("sensor_type") == "Voltage")

      # write voltage sensor data to parquet table in output directory
      (enriched_voltage_data.write.format("parquet")
       .option("mergeSchema","true")
       .mode("overwrite")
       .partitionBy("date","container_id")
       .save(f"{self.output_directory}/enriched_voltage_data")
       )

      self.logger.info("end writing voltage sensor data")
      
    except Exception:
      self.logger.error(msg = "falied writing voltage sensor data", exc_info = True)

  
  def run(self):
    
    try:
      self.logger.info("start sensor data enrichment process")

      # execute the application
      self.persist_data(
        self.enrichment(
        self.extract_sensor_data(),
        self.extract_metadata()
        )
        )

      self.logger.info("successfully completed sensor data enrichment process")
    
    except Exception:
      self.logger.error(msg = "falied sensor data enrichment process", exc_info = True)

class CumulativeThroughput:
  
  """
    Calculate cumulative throughput for each container considering the timestamp

    Parameter
    ---------
    input_directory : generic file path to input data
        Sensor raw data.
        
    output_directory : generic file path for output data
        Metadata.

  """
  
  def __init__(self, input_directory, output_directory):
    self.spark_session = (SparkSession.builder
                          .master("local[*]")
                          .appName("CumulativeThroughput")
                          .config('spark.sql.parquet.int96RebaseModeInWrite', 'LEGACY')
                          .config("spark.sql.legacy.timeParserPolicy","LEGACY")
                          .config("spark.sql.caseSensitive","true")
                          .config("spark.sql.shuffle.partitions", 16)
                          .config("spark.default.parallelism", 16)
                          .getOrCreate())

    self.input_directory = input_directory
    self.output_directory = output_directory
    
    self.logger = logging.getLogger("CumulativeThroughput")

    # Define logg pattern
    self.formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s: %(message)s (%(filename)s:%(lineno)d)',datefmt='%Y-%m-%d %H:%M:%S')
    self.logger.setLevel(logging.INFO)
    self.console = logging.StreamHandler()
    self.console.setFormatter(self.formatter)
    self.logger.addHandler(self.console)
        
  def extract_current_data(self):
    
    """
    Load current sensor data from parquet tables

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains curremnt sensor enriched data.
    """
    
    try:
      self.logger.info("start reading enriched current sensor data")

      # read sensor data from parqute table from the input directory
      current_data = (self.spark_session.read.format("parquet")
                      .load(f"{self.input_directory}/enriched_current_data")
                      .drop("tid","sensor_name","sensor_type","updated_timestamp","date")
                      .withColumnRenamed("value","current")
                      )

      self.logger.info("end reading enriched current sensor data")
      return current_data
    
    except Exception:
      self.logger.error(msg = "falied reading enriched current sensor data", exc_info = True)
  
  def extract_voltage_data(self):

    """
    Load voltage sensor data from parquet tables

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains voltage sensor enriched data.
    """
    
    try:
      self.logger.info("start reading enriched voltage sensor data")

      # read sensor data from parqute table from the input directory
      voltage_data = (self.spark_session.read.format("parquet")
                      .load(f"{self.input_directory}/enriched_voltage_data")
                      .drop("tid","sensor_name","sensor_type","updated_timestamp","date")
                      .withColumnRenamed("value","voltage")
                      )

      self.logger.info("end reading enriched voltage sensor data")
      return voltage_data
    
    except Exception:
      self.logger.error(msg = "falied reading enriched voltage sensor data", exc_info = True)
  
  def calculate_cumulative_throughput(self, current_data, voltage_data):

    """
    Calculate cumulative throught by container and timestamp
    
    hint: To calculate the throughput, you need to multiply current with voltage for the same container and timestamp

    Parameter
    ---------
    current_data : pyspark.sql.dataframe.DataFrame
        Current sensor enriched data.
        
    voltage_data : pyspark.sql.dataframe.DataFrame
        Voltage sensor enriched data.

    Returns
    -------
    pyspark.sql.dataframe.DataFrame
        spark dataframe contains cumulative throughput data.
    """
    
    try:
      self.logger.info("start calculating cumulative throughput")

      # join current and voltage sensor data to catculate throughput
      pre_processed_data = (current_data.join(voltage_data, on = ["container_id","timestamp"], how = 'inner')
                            .withColumn("throughput", f.col("current")*f.col("voltage"))
                            )

      # window function for calculate cumulative throughput
      window_function_cumulative_throuput = (Window.partitionBy('container_id')
                                             .orderBy(f.asc(f.col("timestamp")))
                                             .rowsBetween(Window.unboundedPreceding, 0)
                                             )

      # calculate cumulative throughput
      cumulative_data = (pre_processed_data
                         .withColumn('cumulative_throughput', f.sum('throughput').over(window_function_cumulative_throuput))
                         )

      self.logger.info("end calculating cumulative throughput")
      return cumulative_data
    
    except Exception:
      self.logger.error(msg = "falied calculating cumulative throughput", exc_info = True)
  
  def persist_data(self, cumulative_data):

    """
    Write cumulative throughput data to persistant storage based on container id

    Parameter
    ---------
    cumulative_data : pyspark.sql.dataframe.DataFrame
        Cumulative throughput data.

    Returns
    -------
    parquet table
        Persist data to stotrage using parquet format.
    """
    
    try:
      self.logger.info("start writing cumulative throughput data")

      (cumulative_data
       .withColumn("updated_timestamp", f.current_timestamp())
       .withColumn("date", f.to_date((f.col("timestamp")/1000000).cast('timestamp')))
       .write.format("parquet")
       .option("mergeSchema","true")
       .mode("overwrite")
       .partitionBy("date","container_id")
       .save(f"{self.output_directory}/cumulative_throughput_data")
       )

      self.logger.info("end writing cumulative throughput data")
    
    except Exception:
      self.logger.error(msg = "falied writing cumulative throughput data", exc_info = True)

  
  def run(self):
    
    try:
      self.logger.info("start calculate cumulative throughput process")
      
      # execute the application
      self.persist_data(
        self.calculate_cumulative_throughput(
          self.extract_current_data(),
          self.extract_voltage_data()
          )
        )
          
      self.logger.info("successfully completed calculate cumulative throughpu process")
      
    except Exception:
      self.logger.error(msg = "falied calculate cumulative throughpu process ", exc_info = True)