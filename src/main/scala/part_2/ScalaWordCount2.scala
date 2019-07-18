package part_2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount2 {
  def main(args: Array[String]): Unit = {

    //创建spark配置，设置应用程序名字
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[4]")

    //创建spark执行的入口
    val sc: SparkContext = new SparkContext(conf)

    //指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    val lines: RDD[String] = sc.textFile(args(0))

    //压平切分
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey((x:Int,y:Int)=>x+y)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)

    //将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))

    //释放资源
    sc.stop()




  }
}
