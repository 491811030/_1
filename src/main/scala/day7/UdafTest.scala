package day7

import java.lang

import io.netty.util.internal.chmv8.ConcurrentHashMapV8.DoubleByDoubleToDouble
import org.antlr.v4.runtime.tree.pattern.ParseTreePatternMatcher.StartRuleDoesNotConsumeFullPattern
import org.apache.commons.math3.stat.descriptive.moment.GeometricMean
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object UdafTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JoinTest")
      .master("local[*]")
      .getOrCreate()

    val geomean = new GeoMean

    val range: Dataset[lang.Long] = spark.range(1,11)

    spark.udf.register("gm",geomean)
    range.createTempView("v_range")
    val result1: DataFrame = spark.sql("SELECT gm(id) result FROM v_range ")

    import spark.implicits._
    val result: DataFrame = range.agg(geomean($"id").as("geomean"))

    result.show()
    spark.stop()


  }
}

class GeoMean extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = StructType(List(
    StructField("value",DoubleType)
  ))

  override def bufferSchema: StructType = StructType(List(
    StructField("product",DoubleType),
    StructField("counts",LongType)
  ))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0

    buffer(1) = 0L
  }


  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    //参与运算数据的个数也有更新
    buffer(1) = buffer.getLong(1) + 1L
  }


  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区计算的结果进行相乘
    buffer1(0) =  buffer1.getDouble(0) * buffer2.getDouble(0)
    //每个分区参与预算的中间结果进行相加
    buffer1(1) =  buffer1.getLong(1) + buffer2.getLong(1)
  }
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(0),1.toDouble/buffer.getLong(1))
  }
}
