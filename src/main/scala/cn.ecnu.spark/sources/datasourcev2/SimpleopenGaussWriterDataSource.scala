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
import org.apache.spark.unsafe.types.UTF8String

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

//class SimpleStreamingTable extends Table with SupportsRead {
//  override def name(): String = this.getClass.toString
//
//  override def schema(): StructType = StructType(Array(StructField("value", StringType)))
//
//  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.MICRO_BATCH_READ).asJava
//
//  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new SimpleScanBuilder()
//}

//product_no int, name varchar, price numeric(0)
class openGaussTable extends SupportsWrite with SupportsRead{

  private val tableSchema = new StructType().add("product_no", StringType)

  override def name(): String = this.getClass.toString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_WRITE,
    TableCapability.TRUNCATE).asJava

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = new openGaussWriterBuilder

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = ???
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

class openGaussWriterBuilder extends WriteBuilder{
  override def buildForBatch(): BatchWrite = new openGaussBatchWriter()
}

//class openGaussScanBuilder extends ScanBuilder{
//  override def build(): Scan = new SimpleScan
//}
/*
* TODO
* SimpleScan
*
* */



// simple class to organise the partition
class SimplePartition extends InputPartition

// reader factory
class SimplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new SimplePartitionReader
}
// parathion reader
class SimplePartitionReader extends PartitionReader[InternalRow] {

  val url = "jdbc:postgresql://10.11.6.27:15432/postgres"
  val user = "sparkuser"
  val password = "Enmo@123"
  val table ="products"
  val connection = DriverManager.getConnection(url,user,password)
  val statement = "insert into products (product_no) values (?)"
  val preparedStatement = connection.prepareStatement(statement)


  val values = Array("1", "2", "3", "4", "5")

  var index = 0

  def next = index < values.length

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = Unit

}

/*
* class SimpleScan extends Scan{
  override def readSchema(): StructType =  StructType(Array(StructField("value", StringType)))

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = new SimpleMicroBatchStream()
}

class SimpleOffset(value:Int) extends Offset {
  override def json(): String = s"""{"value":"$value"}"""
}

class SimpleMicroBatchStream extends MicroBatchStream {
  var latestOffsetValue = 0

  override def latestOffset(): Offset = {
    latestOffsetValue += 10
    new SimpleOffset(latestOffsetValue)
  }

  override def planInputPartitions(offset: Offset, offset1: Offset): Array[InputPartition] = Array(new SimplePartition)

  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory()

  override def initialOffset(): Offset = new SimpleOffset(latestOffsetValue)

  override def deserializeOffset(s: String): Offset = new SimpleOffset(latestOffsetValue)

  override def commit(offset: Offset): Unit = {}

  override def stop(): Unit = {}
}


// simple class to organise the partition
class SimplePartition extends InputPartition

// reader factory
class SimplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new SimplePartitionReader
}
// parathion reader
class SimplePartitionReader extends PartitionReader[InternalRow] {

  val values = Array("1", "2", "3", "4", "5")

  var index = 0

  def next = index < values.length

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = Unit

}
* */




