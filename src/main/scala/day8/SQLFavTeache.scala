package day8

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLFavTeache {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("RowNumberDemo").master("local[*]").getOrCreate()

    val topN: Int = args(1).toInt

    val lines: Dataset[String] = spark.read.textFile(args(0))

    import spark.implicits._

    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher: String = line.substring(tIndex)
      val host: String = new URL(line).getHost

      val sIndex: Int = host.indexOf(",")

      val subject: String = host.substring(0, sIndex)

      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")

    //val temp1: DataFrame = spark.sql("SELECT subject,teacher,count(*) counts FROM v_sub_teacher GROUP BY subject,teacher")

    //temp1.createTempView("v_temp_sub_teacher_counts")

    //val temp2: DataFrame = spark.sql(s"SELECT * FROM (SELECT subject,teacher,counts,row_number() over(partition by subject order by counts desc) sub_rk, rank() over (order by counts desc) g_rk FROM v_temp_sub_teacher_counts) temp2 WHERE  sub_rk <= $topN")

    val temp2 = spark.sql(s"SELECT *, dense_rank() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")


    temp2.show()



    spark.stop()
  }
}
