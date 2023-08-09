package com.appsflyer.bdnd

import org.apache.iceberg.{
  CatalogProperties,
  DataFile,
  DataFiles,
  MetricsConfig,
  Table
}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.parquet.{Parquet, ParquetUtil}
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader

import scala.jdk.CollectionConverters._
class ParquetInputStreamAdapter(
    delegate: org.apache.iceberg.io.SeekableInputStream
) extends org.apache.parquet.io.DelegatingSeekableInputStream(delegate) {

  override def getPos: Long = delegate.getPos

  override def seek(newPos: Long): Unit = delegate.seek(newPos)
}

class ParquetInputFile(file: org.apache.iceberg.io.InputFile)
    extends org.apache.parquet.io.InputFile {

  override def getLength: Long = file.getLength

  override def newStream() = new ParquetInputStreamAdapter(file.newStream())
}

object NewFileAppender {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def buildGlueCatalog(
      catalogName: String,
      warehouseLocation: String
  ): GlueCatalog = {
    val catalog = new GlueCatalog
    val props = Map(
      CatalogProperties.CATALOG_IMPL -> classOf[GlueCatalog].getName,
      CatalogProperties.WAREHOUSE_LOCATION -> warehouseLocation,
      CatalogProperties.FILE_IO_IMPL -> classOf[S3FileIO].getName
    ).asJava
    catalog.initialize(catalogName, props)
    catalog
  }

  def getIcebergTableByName(
      namespace: String,
      tableName: String,
      catalog: GlueCatalog
  ): Table = {
    log.info(s"Loading table $tableName from namespace $namespace")
    val tableID = TableIdentifier.of(namespace, tableName)
    catalog.loadTable(tableID)
  }

  private def buildParquetFile(
      parquetFilePath: String,
      table: Table,
      partitionPath: String
  ) = {
    // get metrics
    val inputFile = table.io().newInputFile(parquetFilePath)
    val hadoopInputFile = new ParquetInputFile(inputFile)
    val fileMetrics =
      ParquetUtil.fileMetrics(inputFile, MetricsConfig.forTable(table))
    val parquetMetadata =
      ParquetFileReader.open(hadoopInputFile).getFooter
    val splitOffsets =
      ParquetUtil.getSplitOffsets(parquetMetadata)

    DataFiles
      .builder(table.spec())
      .withSplitOffsets(splitOffsets)
      .withPath(parquetFilePath)
      .withInputFile(inputFile)
      .withFormat("parquet")
      .withMetrics(fileMetrics)
      .withPartitionPath(partitionPath)
      .build()
  }

  def main(args: Array[String]): Unit = {
    val s3TableLocation = "s3://******"
    val tableName = "p_test_1"
    val databaseName = "sampleDB"
    val parquetFilePath ="s3://newBucket/somefolder/copy5.parquet"
  
    val icebergCatalog = buildGlueCatalog("iceberg", s3TableLocation)
    val table = getIcebergTableByName(databaseName, tableName, icebergCatalog)
    val dataFile = buildParquetFile(parquetFilePath, table, "tier=A")
    table.newAppend().appendFile(dataFile).commit()


  }

}
