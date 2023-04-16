package com.appsflyer.bdnd

import org.apache.iceberg.{CatalogProperties, DataFile, Table}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.data.parquet.{GenericParquetReaders, GenericParquetWriter}
import org.apache.iceberg.expressions.{UnboundPredicate, _}
import org.apache.iceberg.io.{CloseableIterable, DataWriter}
import org.apache.iceberg.parquet.Parquet
import java.util.UUID
import scala.jdk.CollectionConverters._


case class ProgramArgs(catalogName: String, locationS3: String, icebergNamespace: String, icebergTableName: String)

object IcebergRewriter {

  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  def getGlueCatalog(catalogName: String, warehouseLocation: String): GlueCatalog =  {
    val catalog = new GlueCatalog
    val props = Map(
      CatalogProperties.CATALOG_IMPL -> classOf[GlueCatalog].getName,
      CatalogProperties.WAREHOUSE_LOCATION -> warehouseLocation,
      CatalogProperties.FILE_IO_IMPL -> classOf[S3FileIO].getName
    ).asJava
    catalog.initialize(catalogName, props)
    catalog
  }

  def getIcebergTableByName(namespace: String, tableName: String, catalog: GlueCatalog): Table =  {
    val tableID = TableIdentifier.of(namespace, tableName)
    catalog.loadTable(tableID)
  }

  private def getDataFiles(table: Table, predicate: Option[UnboundPredicate[String]]): List[DataFile] = {
    val filteredScan = predicate match {
      case Some(filter) => table.newScan().filter(filter)
      case None => table.newScan()
    }
    filteredScan.planFiles().asScala.toList.map(_.file())
  }

  private def getDataWriter(table: Table, sampleFile:DataFile): DataWriter[GenericRecord] = {
    val currentFileLocation  = sampleFile.path().toString
    val currentPartition = sampleFile.partition()
    val partitionPath = currentFileLocation.substring(0, currentFileLocation.lastIndexOf("/"))
    val filePath = s"$partitionPath/${UUID.randomUUID().toString}.parquet"
    log.info(s"compacting files to:$filePath")
    val outputFile = table.io().newOutputFile(filePath)
   Parquet
      .writeData(outputFile)
      .schema(table.schema())
      .createWriterFunc(GenericParquetWriter.buildWriter)
      .overwrite()
      .withPartition(currentPartition)
      .withSpec(table.spec())
      .build
  }

  //noinspection DuplicatedCode
  private def getDataReader(table: Table, filePath:String): CloseableIterable[GenericRecord] = {
    val inputFile = table.io().newInputFile(filePath)
    Parquet.read(inputFile)
      .project(table.schema())
      .createReaderFunc(fileSchema => GenericParquetReaders.buildReader(table.schema(), fileSchema))
      .build()
  }

  private def compactPartitionFiles(dataFiles: List[DataFile], table: Table): DataFile = {
    val dataWriter = getDataWriter(table, dataFiles.head)
    val outputFiles = dataFiles.map(_.path().toString)
    outputFiles.foreach(parquetFile => {
      val dataReader = getDataReader(table, parquetFile)
      log.info(s"reading  file:$parquetFile")
      dataReader.iterator().asScala.foreach(dataWriter.write)
      dataReader.close()
    })
    dataWriter.close()
    log.info(s" output file:${dataWriter.toDataFile.path} written")
    dataWriter.toDataFile
  }

  def commitCompaction(dataFilesToDelete: List[DataFile], dataFilesToAdd: List[DataFile], table: Table): Unit = {
    table.newRewrite()
      .rewriteFiles(dataFilesToDelete.toSet.asJava, dataFilesToAdd.toSet.asJava)
      .commit()
  }

  def main(args: Array[String]): Unit = {
    log.info("IcebergRewriter starting")
    val programArgs = ProgramArgs(args(0), args(1), args(2), args(3))
    val catalog = getGlueCatalog(programArgs.catalogName, programArgs.locationS3)
    val table = getIcebergTableByName(programArgs.icebergNamespace, programArgs.icebergTableName, catalog)
    val filter = Expressions.equal("tier", "B")
    val dataFiles = getDataFiles(table, Some(filter))
    // partition files before compaction
    dataFiles.foreach(df => log.info(df.path().toString))
    // compact the partition files
    val compactedFile = compactPartitionFiles(dataFiles,  table)
    // commit the rewrite
    commitCompaction(dataFiles, List(compactedFile), table)
    // now show the partition files after compaction
    getDataFiles(table, Some(filter)).foreach(df => log.info(df.path().toString))

  }


}
