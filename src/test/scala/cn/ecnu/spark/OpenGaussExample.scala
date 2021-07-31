package cn.ecnu.spark


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.FlatSpec
import java.sql.DriverManager
import java.util.Properties

import org.scalatest.Matchers.convertToAnyShouldWrapper

class OpenGaussExample extends FlatSpec {


  val testTableName = "products"

  "Simple data source" should "read" in{
    val sparkSession = SparkSession.builder
      .master("local[2]")
      .appName("example")
      .getOrCreate()

    val simpleDf = sparkSession.read
      .format("cn.ecnu.spark.sources.datasourcev2.simple")
      .load()

    simpleDf.show()
    println(
      "number of partitions in simple source is " + simpleDf.rdd.getNumPartitions)
  }


  "PostgreSQL data source" should "read table" in  {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OpenGaussReaderJob")
      .getOrCreate()

    val simpleRead = spark
      .read
      .format("cn.ecnu.spark.sources.opengauss")
      .option("url", "jdbc:postgresql://10.11.6.27:15432/postgres")
      .option("user", "sparkuser")
      .option("password", "Enmo@123")
      .option("tableName", testTableName)
      .option("partitionSize", 10)
      .option("partitionColumn", "product_no")
      .load()
      .show()

    //    print("simpleRead "+simpleRead)
    spark.stop()
  }

  "PostgreSQL data source" should "write table" in {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("OpenGaussWriterJob")
      .getOrCreate()

    import spark.implicits._

    val df = (60 to 70).map(_.toLong).toDF("product_no")

    df
      .write
      .format("cn.ecnu.spark.sources.opengauss")
      .option("url", "jdbc:postgresql://10.11.6.27:15432/postgres")
      .option("user", "sparkuser")
      .option("password", "Enmo@123")
      .option("tableName", testTableName)
      .option("partitionSize", 10)
      .option("partitionColumn", "product_no")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }



//  def connection(c: PostgreSQLContainer) = {
//    Class.forName(c.driverClassName)
//    val properties = new Properties()
//    properties.put("user", c.username)
//    properties.put("password", c.password)
//    DriverManager.getConnection(c.jdbcUrl, properties)
//  }

  object Queries {
    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"

    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")

    lazy val insertDataQuery = s"INSERT INTO $testTableName VALUES $testValues;"
  }

}

