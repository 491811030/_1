package day8

import org.apache.spark.sql.{DataFrame, SparkSession}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("CsvDataSource").master("local[*]").getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._


    //spark.sql.autoBroadcastJoinThreshold=-1
    spark.conf.set("spark.sql.autoBroadcaseJoinThreshold",-1)
    //spark.conf.set("spark.sql.preferSortMergeJoin",ture)


    val df1: DataFrame = Seq(
      (0, "playing"),
      (1, "with"),
      (2, "join")
    ).toDF("id", "token")

    val df2: DataFrame = Seq(
      (0, "p"),
      (1, "w"),
      (2, "s")
    ).toDF("aid", "atokrn")

    df2.repartition()

    df1.cache().count()

    val result: DataFrame = df1.join(df2,$"id"===$"aid")

    result.explain()

    result.show()

    spark.stop()


  }
}
