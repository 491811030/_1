package game
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetGameKPI {
  def main(args: Array[String]): Unit = {
    val queryTime = "2016-02-01 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(1)


    val spark: SparkSession = SparkSession.builder().appName("DataSetGameKPI").master("local[*]").getOrCreate()

    import spark.implicits._

    val lines: Dataset[String] = spark.read.textFile(args(0))

    val filterUtils = new FilterUtils_V2

    val splited: Dataset[Array[String]] = lines.map(_.split("[|]"))

    val dnuFf: DataFrame = splited.filter(fields => {
      filterUtils.filterByType(fields, EventType.REGISTER)
    }).map(fields => {
      (fields(0), fields(1), fields(2), fields(3), fields(4), fields(5), fields(6), fields(7), fields(8))

    }).toDF("type", "time", "ip", "username", "job", "gender", "level", "money", "gold")
    dnuFf.createTempView("dnu")

    val dnu: Long = dnuFf.count

    val t1 = endTime

    val t2 = TimeUtils.getCertainDayTime(2)

    val nextDayLoginDf: DataFrame = splited.filter(fields => {
      filterUtils.filterByTypeAndTime(fields, EventType.Login, t1, t2)
    }).map(fields => {
      fields(3)
    }).distinct().toDF("username")
    nextDayLoginDf.createTempView("next_day_login")

     val r: DataFrame = spark.sql("SELECT count(*) from dnu join next_day_login on (dnu.username = next_day_login.username")

    r.show()

    spark.stop()









  }
}
