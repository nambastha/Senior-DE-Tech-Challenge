package com.challenge.section1

import org.apache.spark.sql.functions.{col, date_format, expr, split, to_date, when}
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
    val app_dataset_df: DataFrame = spark.read
      .option("header","true")
      .option("sep",",")
      .csv("/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/input/applications_dataset_1.csv"
        ,"/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/input/applications_dataset_2.csv")

    //show the sampled data
    app_dataset_df.show()


    app_dataset_df.createOrReplaceTempView("app_dataset")


    println("count "+app_dataset_df.count())

    val df = app_dataset_df
      .withColumn("first_name",split(col("name")," ").getItem(0))
      .withColumn("last_name",split(col("name")," ").getItem(1))
      .withColumn("date_of_birth",
        when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy-MM-dd"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"yyyy MM dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MM dd"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"MM/dd/yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"MM/dd/yyyy"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"yyyy MMMM dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MMMM dd"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"MM-dd-yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"MM-dd-yyyy"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"yyyy-MM-dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy-MM-dd"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"dd-MM-yyyy").isNotNull, date_format(to_date(col("date_of_birth"),"dd-MM-yyyy"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"yyyy/MM/dd").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy/MM/dd"),"YYYYMMDD"))
          .when(to_date(col("date_of_birth"),"yyyy MMMM dd E").isNotNull, date_format(to_date(col("date_of_birth"),"yyyy MMMM dd E"),"YYYYMMDD")))
      .filter(col("name").isNotNull)
    //.drop("name")

    df.show()

    println("count "+df.count)

    val df2 = df.na.drop(Seq("name"))
    df2.show()

    println("count "+df2.count)
  }

}
