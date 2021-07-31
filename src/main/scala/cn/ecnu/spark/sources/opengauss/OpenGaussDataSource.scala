package cn.ecnu.spark.sources.opengauss

import java.sql.DriverManager
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DefaultSource extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = OpenGaussTable.schema

  override def getTable(
                         schema: StructType,
                         partitioning: Array[Transform],
                         properties: util.Map[String, String]
                       ): Table = new OpenGaussTable(properties.get("tableName")) // TODO: Error handling
}

class OpenGaussTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = OpenGaussTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new OpenGaussScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new OpenGaussWriteBuilder(info.options)
}

object OpenGaussTable {
  val schema: StructType = new StructType().add("product_no", IntegerType)
}

// Добавляет две опции в свойства коннектора
case class ConnectionProperties(url: String, user: String, password: String, tableName: String, partitionColumn: String, partitionSize: Int)

/** Read */

class OpenGaussScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new OpenGaussScan(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"), options.get("partitionColumn"), options.get("partitionSize").toInt
  ))
}

//class OpenGaussPartition(val url:String,
//                        val user: String,
//                        val password: String,
//                        val part: Long,
//                        val steps: Long) extends InputPartition

class OpenGaussPartition extends InputPartition

class OpenGaussScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  override def readSchema(): StructType = OpenGaussTable.schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
//    val connection = DriverManager.getConnection(connectionProperties.url, connectionProperties.user, connectionProperties.password)
//    val statement = connection.createStatement()
//    val resultSet = statement.executeQuery(s"select count(*) as count from ${connectionProperties.tableName}")
//    val count = resultSet.getInt("count")
//    connection.close()
//    val step = count/connectionProperties.partitionSize
//    (for (parts <- 0 to count by step) yield new OpenGaussPartition(connectionProperties.url, connectionProperties.user, connectionProperties.password, parts, step)).toArray
    Array(new OpenGaussPartition)
  }

  override def createReaderFactory(): PartitionReaderFactory = new OpenGaussPartitionReaderFactory(connectionProperties)
}

class OpenGaussPartitionReaderFactory(connectionProperties: ConnectionProperties)
  extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new OpenGaussPartitionReader(connectionProperties)
}

class OpenGaussPartitionReader(connectionProperties: ConnectionProperties) extends PartitionReader[InternalRow] {
  private val connection = DriverManager.getConnection(
    connectionProperties.url, connectionProperties.user, connectionProperties.password
  )
  private val statement = connection.createStatement()
  private val resultSet = statement.executeQuery(s"select * from ${connectionProperties.tableName}")

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getInt(1))

  override def close(): Unit = connection.close()
}

/** Write */

class OpenGaussWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new OpenGaussBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName"), options.get("partitionColumn"), options.get("partitionSize").toInt
  ))
}

class OpenGaussBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new OpenGaussDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class OpenGaussDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new OpenGaussWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class OpenGaussWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into products (product_no) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getInt(0)
    preparedStatement.setInt(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = connection.close()
}


