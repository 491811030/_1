package day8

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("HiveOnSpark").master("local[*]").enableHiveSupport().getOrCreate()

    val sql: DataFrame = spark.sql("CREATE TABLE niu (id bigint, name string)")

    sql.show()

    spark.close()


  }
}
