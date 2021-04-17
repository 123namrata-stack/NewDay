package com.newday

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object moviesRatings {

  def main(args: Array[String]): Unit = {

   val spark =  SparkSession.builder().master("local").appName("movies ratings").getOrCreate()

    val moviesTmpDF = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ":").load("C:\\namu\\learning\\NewDay\\ml-1m\\movies.dat")
      .toDF("movieId","nulls","title","nullss","genre").select("movieId","title","genre")

    val moviesDF = moviesTmpDF.withColumn("title", trim(col("title")))
      .withColumn("genre", trim(col("genre")))

    val ratingsDF = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ":").load("C:\\namu\\learning\\NewDay\\ml-1m\\ratings.dat")
      .toDF("userId","null","movieId","nullss","rating","nullsss","timestamp")
      .select("userId","movieId","rating","timestamp")

     val userDF = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ":").load("C:\\namu\\learning\\NewDay\\ml-1m\\users.dat")
       .toDF("userId","nulls","gender","nullss","number","nul","age","nullss","score")
       .select("userId","gender","number","age","score")

     val joinedDF = moviesDF.join(ratingsDF,
       moviesDF.col("movieId") === ratingsDF.col("movieId"),"left")
       .drop(ratingsDF.col("movieId"))

     val movieRatingsDF = joinedDF.groupBy(col("movieId"),col("title"),col("genre"))
      .agg(max("rating").as("max_ratings"),
        min("rating").as("min_ratings"),round(avg("rating"),2).as("avg_ratings"))

    val rnkFunc = Window.partitionBy(ratingsDF.col("userID")).orderBy(ratingsDF.col("rating").desc)

    val rankDF = ratingsDF.withColumn("rnk", rank().over(rnkFunc))

    val filteredDF = rankDF.filter(col("rnk")<4)

    val newjoinedDF = filteredDF.join(moviesDF,
      filteredDF.col("movieId") === moviesDF.col("movieId"),"left")
      .drop(filteredDF.col("movieId"))

    val topmoviesDF = newjoinedDF.groupBy(("userID")).agg(collect_list("title").as("top_movies"))

    topmoviesDF.show()
  }

}
