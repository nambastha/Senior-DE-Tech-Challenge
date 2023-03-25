package com.challenge.section1

import org.apache.spark.sql.functions.{col, current_date, date_format, datediff, expr, lit, months_between, round, split, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Solution1 {
  def main(args: Array[String]): Unit = {

    // creating a spark session
    val spark: SparkSession = SparkSession.builder()
      .master("local[3]") // 3 threads. 1 driver and 2 executors // specifies spark cluster manager
      .appName("Spark")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    // reading data from both csv files
    val appDatasetDf: DataFrame = spark.read
      .option("recursiveFileLookup","true")
      .option("header","true")
      .option("sep",",")
      .csv("/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/input/")

    appDatasetDf.createOrReplaceTempView("appDataset")

    println("count "+appDatasetDf.count())

    import spark.implicits._

    val splitNamesDf = appDatasetDf
      .withColumn("first_name",split(col("name")," ").getItem(0))
      .withColumn("last_name",split(col("name")," ").getItem(1))

    splitNamesDf.show()

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


    formattedDateDf.createOrReplaceTempView("formatDate")

    val nullNamesDf = formattedDateDf.filter(col("name").isNull)

    val ageDf = formattedDateDf.withColumn("age",round(months_between(to_date(lit("20220101"),"yyyyMMdd")
      ,to_date(col("date_of_birth"),"yyyyMMdd"))/lit(12),2) )


    ageDf.show()


  }

}
