package day7

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JsonDataSource {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("JdbcDataSource").master("local[*]").getOrCreate()

    import spark.implicits._
    val json: DataFrame = spark.read.json("/Users/zx/Desktop/json")


    val filtered: Dataset[Row] = json.where($"age"<=500)

    filtered.printSchema()

    filtered.show()

    spark.stop()

  }
}
