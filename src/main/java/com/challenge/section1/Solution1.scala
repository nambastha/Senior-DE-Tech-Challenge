package com.challenge.section1

import org.apache.spark.sql.functions.{col, date_format, floor, length, lit, months_between, regexp_replace, round, sha2, split, substring, to_date,when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

object Solution1 {
  def main(args: Array[String]): Unit = {

    // creating a spark session
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      //.master("local[3]") // 3 threads. 1 driver and 2 executors : specifies spark cluster manager
      .appName("Spark")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    // reading data from 'input' folder
    val appDatasetDf: DataFrame = spark.read
      .option("recursiveFileLookup","true")
      .option("header","true")
      .option("sep",",")
      .csv("/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/input/")

    // filtering out the names with null values
    val notNullNamesDf = appDatasetDf
      .filter(col("name").isNotNull)

    // splitting the given name into first and last name
    val splitNamesDf = notNullNamesDf
      .withColumn("first_name",split(col("name")," ").getItem(0))
      .withColumn("last_name",split(col("name")," ").getItem(1))

    // formatting the date to yyyyMMdd
    val formattedDateDf = splitNamesDf.withColumn("date_of_birth",
        when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy-MM-dd"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"yyyy MM dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MM dd"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"MM/dd/yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"MM/dd/yyyy"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"yyyy MMMM dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MMMM dd"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"MM-dd-yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"MM-dd-yyyy"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy-MM-dd"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"dd-MM-yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"dd-MM-yyyy"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"yyyy/MM/dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy/MM/dd"),"yyyyMMdd"))
          .when(to_date(col("date_of_birth"),"yyyy MMMM dd E").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MMMM dd E"),"yyyyMMdd")))


    // calculating the age based on the date of birth till 01-01-2022
    val ageDf = formattedDateDf
      .withColumn("age",round(months_between(to_date(lit("20220101"),"yyyyMMdd")
      ,to_date(col("date_of_birth"),"yyyyMMdd"))/lit(12),2) )

    // converting the age to nearest integer digit
    val above18Df = ageDf
      .withColumn("above_18",floor(col("age")))

    // generating SHA256 hash for date_of_birth
    val hashedDf = above18Df
      .withColumn("hash", substring(sha2(col("date_of_birth"), 256), 1, 5))

    // generating membership id
    val membershipIdDf = hashedDf
      .withColumn("membership_id",functions.concat(col("last_name"),lit('_'), col("hash")))
      .drop("name","age","hash")

    // removing white spaces between mobile numbers
    val trimValidAppDf = membershipIdDf
      .withColumn("mobile_no", regexp_replace(col("mobile_no"), "\\s", ""))

    // filtering based on mobile number length = 8
    val validMobDf = trimValidAppDf
      .filter(length(col("mobile_no").cast("int")) === 8)

    // filtering out the records where email is of type .com or .net
    val validEmailDf = validMobDf
      .filter(col("email").like("%.com") || col("email").like("%.net"))

    // filtering based on age equal or above 18
    val successfulDf = validEmailDf
      .filter(col("above_18").cast("int") >= 18)

    successfulDf.write.mode(SaveMode.Overwrite).csv("/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/successful/")

    // creating temp table/view to query for unsuccessful records
    trimValidAppDf.createOrReplaceTempView("trimValidAppDf")

    val query =
      """SELECT * FROM trimValidAppDf
        |WHERE email NOT LIKE '%.com'
        |AND email NOT LIKE '%.net'
        |OR length(mobile_no) <> 8
        |OR above_18 < 18
        |OR first_name IS NULL
        |OR last_name IS NULL """.stripMargin

    val unsuccessfulDf = spark.sql(query)
    unsuccessfulDf.write.mode(SaveMode.Overwrite).csv("/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/unsuccessful/")

  }
}
