package day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-4:9000/words")

    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))


    words.createTempView("v_wc")

    val result: DataFrame = spark.sql("SELECT value word,COUNT(*) counts FROM v_ec GROUP BY word ORDER BY counts DESC")

    result.show()
    spark.stop()
    

  }
}
