package day7

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhang,china","2,landuan,usa","3,laoyang,jp"))

    val df1: DataFrame = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val nationCode: String = fields(2)
      (id, name, nationCode)

    }).toDF("id", "name", "nation")

    val nations: Dataset[String] = spark.createDataset(List("china,zhongguo","usa,meiguo"))

    val df2: DataFrame = nations.map(l => {
      val fields: Array[String] = l.split(",")

      val ename: String = fields(0)

      val cname: String = fields(1)
      (ename, cname)
    }).toDF()

    df1.createTempView("v_users")
    df2.createTempView("v_nations")

    spark.sql("SELECT name,cname FROM v_user JOIN v_nations ON nations = ename")

    val r: DataFrame = df1.join(df2,$"nation" ===$"ename","left_outer")

    r.show()
    spark.stop()


  }


}
