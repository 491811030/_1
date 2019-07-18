package day5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val users = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 98","laoyang 28 99")

    val lines: RDD[String] = sc.parallelize(r)

    val userRDD: RDD[User] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val fv: Int = fields(2).toInt
      new User(name, age, fv)

    })
    val sorted: RDD[User] = userRDD.sortBy(u=>u)

    val r: Array[User] = sorted.collect()

    println(r.toBuffer)

    sc.stop()

  }



}
class User(val name:String,val age:Int,val fv:Int) extends Ordered[User] with Serializable{
  override def compare(that: User): Int = {
    if(this.fv==that.fv){
      this.age - that.age
    }
    else{
      -(this.fv-that.fv)
    }
  }
  override def toString: String = s"name: $name,age:$age,fv:$fv"

}
