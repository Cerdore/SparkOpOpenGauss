package cn.ecnu.spark.sources.datasourcev2

import org.apache.spark.Partition
import org.apache.spark.sql.{SaveMode, SparkSession}
//import shapeless.Tuple

object DataSourceV2Example {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .getOrCreate()

//    val simpleDf = sparkSession.read
//      .format("cn.ecnu.spark.sources.datasourcev2.simple")
//      .load()
//
//    simpleDf.show()
//    println(
//      "number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)



    val simpleMysqlDf = sparkSession.createDataFrame(Seq(
      Tuple1("5"),
      Tuple1("7")
    )).toDF("product_no")

    //write examples
    simpleMysqlDf.write
      .format(
        "cn.ecnu.spark.sources.datasourcev2.simpleopenGausswriter")
      .mode(SaveMode.Append)
      .save()

   /* simpleMysqlDf.write
      .format(
        "cn.ecnu.examples.sparktwo.datasourcev2.mysqlwithtransaction")
      .save()

    val simplePartitoningDf = sparkSession.read
      .format(
        "cn.ecnu.examples.sparktwo.datasourcev2.partitionaffinity")
      .load()

    val dfRDD = simplePartitoningDf.rdd
    val baseRDD =
      dfRDD.dependencies.head.rdd.dependencies.head.rdd.dependencies.head.rdd

    val partition = baseRDD.partitions(0)
    val getPrefferedLocationDef = baseRDD.getClass
      .getMethod("getPreferredLocations", classOf[Partition])
    val preferredLocation = getPrefferedLocationDef
      .invoke(baseRDD, partition)
      .asInstanceOf[Seq[String]]
    println("preferred location is " + preferredLocation)

    */

    sparkSession.stop()

  }
}
