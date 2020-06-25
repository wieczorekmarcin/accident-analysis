import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Accidents extends App {

  def removeAccidentsColumns(dataset: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]): org.apache.spark.sql.DataFrame = {
    return dataset
      .drop("End_Time")
      .drop("Start_Lat")
      .drop("Start_Lng")
      .drop("End_Lat")
      .drop("End_Lng")
      .drop("Number")
      .drop("Street")
      .drop("Side")
      .drop("Amenity")
      .drop("Bump")
      .drop("Crossing")
      .drop("Give_Way")
      .drop("Junction")
      .drop("No_Exit")
      .drop("Railway")
      .drop("Roundabout")
      .drop("Station")
      .drop("Stop")
      .drop("Traffic_Calming")
      .drop("Traffic_Signal")
      .drop("Turning_Loop")
      .drop("Sunrise_Sunset")
      .drop("Civil_Twilight")
      .drop("Nautical_Twilight")
      .drop("Astronomical_Twilight")
  }

  def createDataIds(dataFrame: org.apache.spark.sql.DataFrame, colName: String): org.apache.spark.sql.DataFrame = {
    return dataFrame.withColumn(colName, monotonically_increasing_id())
  }

  def extractColumn(dataFrame: org.apache.spark.sql.DataFrame, oldColumnName: String, newColumnName: String, regexp_patten: String): org.apache.spark.sql.DataFrame = {
    return dataFrame.withColumn(newColumnName, regexp_extract(dataFrame(oldColumnName), regexp_patten, 0))
  }

  def publishDataTable(dataFrame: org.apache.spark.sql.DataFrame, tableName: String) = {
    dataFrame.createOrReplaceTempView(tableName)
  }

  def createDateTable(): org.apache.spark.sql.DataFrame = {
    sql("CREATE TABLE IF NOT EXISTS date(date string)")
    sql("INSERT INTO TABLE date SELECT Start_Time as date from accidents")
    sql("INSERT INTO TABLE date SELECT date date from weather")

    val dateAsString = sql("SELECT * FROM date")
    val datesConverted = extractColumn(dateAsString, "date", "dateAsString", "([12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))")
    val dateCleared = datesConverted.drop("date")

    publishDataTable(dateCleared, "date")

    val dateGroupped = sql("SELECT dateAsString from date GROUP BY dateAsString ORDER BY dateAsString DESC")
    return dateGroupped.withColumnRenamed("dateAsString", "date");
  }

  def readFile(path: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    return spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path)
  }

  def readFileWithSchema(path: String, schema: StructType, delimiter: String): org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = {
    return spark.read.format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      option("delimiter", ",").
      schema(schema).
      csv(path)
  }

  // Start - ETL
  val geoDataCentral = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/geoDataCentral.csv")
  val geoDataEastern = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/geoDataEastern.csv")
  val geoDataMountain = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/geoDataMountain.csv")
  val geoDataPacific = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/geoDataPacific.csv")

  val schema = StructType(
    List(
      StructField("date_and_airport", StringType, true),
      StructField("wind_chill", StringType, true),
      StructField("humidity", StringType, true),
      StructField("pressure", StringType, true),
      StructField("visibility", StringType, true),
      StructField("wind_direction", StringType, true),
      StructField("wind_speed", StringType, true),
      StructField("precipitation", StringType, true),
      StructField("weather_condition", StringType, true)
    )
  )

  val weather = readFileWithSchema("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/weather.txt", schema, ",")

  val mainDataCentral = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/mainDataCentral.csv")
  val mainDataEastern = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/mainDataEastern.csv")
  val mainDataMountain = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/mainDataMountain.csv")
  val mainDataPacific = readFile("/home/marcinwieczorek/Pulpit/BigDataProcessing-Project2/us-accidents/mainDataPacific.csv")
  // End - ETL

  // Start - Transform Data
  val unitedGeoDataWithoutId = geoDataCentral.toDF()
    .union(geoDataEastern.toDF())
    .union(geoDataMountain.toDF())
    .union(geoDataPacific.toDF())

  val geoData = createDataIds(unitedGeoDataWithoutId, "geography_id")
  publishDataTable(geoData, "geography")

  val weatherDF = weather.toDF()
  val weatherDFWithAirportCode = extractColumn(weatherDF, "date_and_airport", "airport_code", "(?<=airport )(.*)(?= the )")
  val weatherDFDate = extractColumn(weatherDFWithAirportCode, "date_and_airport", "date", "(?<=On )(.*)(?= at the weather)")
  val weatherDFDateConverted = weatherDFDate.withColumn("date", col("date").cast(TimestampType))

  val weatherDFResult = weatherDFDateConverted.drop("date_and_airport")
  val weatherData = createDataIds(weatherDFResult, "weather_id")

  val unitedMainData =
    mainDataCentral.toDF()
      .union(mainDataEastern.toDF())
      .union(mainDataMountain.toDF())
      .union(mainDataPacific.toDF())

  val accidentsDataWithRemovedColmns = removeAccidentsColumns(unitedMainData)
  val accidentsData = accidentsDataWithRemovedColmns.withColumnRenamed("Distance(mi)", "Distance")

  publishDataTable(accidentsData, "accidents")
  publishDataTable(weatherData, "weather")
  publishDataTable(geoData, "geography")

  val dateData = createDateTable()
  publishDataTable(dateData, "date")

  val joinedAccidentsAndWeather = sql("" +
    "SELECT accidents.*, weather.weather_id " +
    "FROM accidents " +
    "LEFT JOIN weather ON accidents.Start_Time = weather.date " +
    "AND accidents.airport_code = weather.Airport_Code ORDER BY accidents.ID")
  publishDataTable(joinedAccidentsAndWeather, "accidents")

  val joinedAccidentsAndWeatherAndGeography = sql("" +
    "SELECT accidents.*, geography.geography_id " +
    "FROM accidents " +
    "LEFT JOIN geography ON accidents.Zipcode = geography.Zipcode")
  publishDataTable(joinedAccidentsAndWeather, "accidents")

  val joinedResultData = sql("" +
    "SELECT date, accidents.ID as accidents_id, geography.geography_id as geography_id, accidents.Severity as accidents_severity, accidents.Distance as accidents_distance " +
    "FROM accidents " +
    "INNER JOIN geography ON geography.Zipcode=accidents.Zipcode " +
    "INNER JOIN date ON accidents.Start_Time LIKE CONCAT('%', date.date, '%')")
  publishDataTable(joinedResultData, "result")

  val finalData = sql("" +
    "SELECT max(date) as date, max(geography_id) as geography_id, count(*) AS sum_accidents, AVG(accidents_severity) AS avg_accidents_severity, AVG(accidents_distance) AS avg_accidents_distance " +
    "FROM result " +
    "GROUP BY date, geography_id  " +
    "ORDER BY sum_accidents DESC")
  publishDataTable(finalData, "result")

  dateData.show(50, false)
  weatherData.show(50, false)
  geoData.show(50, false)
  joinedAccidentsAndWeatherAndGeography.show(50, false)
  //sql("select * from accidents where weather_id is not null").show()
  finalData.show(50, false)

  // End - Transform Data
}