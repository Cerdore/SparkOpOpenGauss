package cn.ecnu.spark.sources.datasourcev2.simpleopenGausswriter

import java.sql.DriverManager
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  *
  */

class DefaultSource extends TableProvider{
    override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table ={
      new openGaussTable
    }
}

//product_no int, name varchar, price numeric(0)
class openGaussTable extends SupportsWrite{

  private val tableSchema = new StructType().add("product_no", StringType)

  override def name(): String = this.getClass.toString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_WRITE,
    TableCapability.TRUNCATE).asJava

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = new openGaussWriterBuilder
}

class openGaussWriterBuilder extends WriteBuilder{
  override def buildForBatch(): BatchWrite = new openGaussBatchWriter()
}

class openGaussBatchWriter extends BatchWrite{
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new
  openGaussDataWriterFactory

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class openGaussDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new openGaussWriter()
}



object WriteSucceeded extends WriterCommitMessage

class openGaussWriter extends DataWriter[InternalRow] {
  val url = "jdbc:postgresql://10.11.6.27:15432/postgres"
  val user = "sparkuser"
  val password = "Enmo@123"
  val table ="products"

  val connection = DriverManager.getConnection(url,user,password)
  val statement = "insert into products (product_no) values (?)"
  val preparedStatement = connection.prepareStatement(statement)


  override def write(record: InternalRow): Unit = {
    val value = record.getString(0)
    preparedStatement.setString(1,value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = {}
}








