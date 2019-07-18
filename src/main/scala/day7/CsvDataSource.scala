package day7

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("CsvDataSource").master("local[*]").getOrCreate()

    val csv: DataFrame = spark.read.csv("/Users/zx/Desktop/csv")

    csv.printSchema()

    val pdf: DataFrame = csv.toDF("id","name","age")

    pdf.show()

    spark.stop()



  }
}
