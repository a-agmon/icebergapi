import com.typesafe.scalalogging.Logger
import org.apache.iceberg.{CatalogProperties, Table, TableScan}
import org.apache.iceberg.aws.glue.GlueCatalog
import org.apache.iceberg.aws.s3.S3FileIO
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.expressions.{Expression, Expressions}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object Main {

  private val logger = Logger("IcebergQueryService")

  implicit class JsonResult(resultSet:ResultSet) {
      def toStringList:List[String] = {
        new Iterator[String] {
          override def hasNext: Boolean = resultSet.next()
          override def next(): String = resultSet.getString(1)
        }.toList
      }
  }

  private def getGlueCatalog(): Try[GlueCatalog] = Try{
    val catalog =  new GlueCatalog
    val props = Map(
      CatalogProperties.CATALOG_IMPL -> classOf[GlueCatalog].getName,
      CatalogProperties.WAREHOUSE_LOCATION -> "s3://Doesnt_Matter_In_This_Context",
      CatalogProperties.FILE_IO_IMPL -> classOf[S3FileIO].getName
    ).asJava
    catalog.initialize("af", props)
    catalog
  }

  private def getIcebergTableByName(namespace: String, tableName: String, catalog: GlueCatalog): Try[Table] = Try{
    val tableID = TableIdentifier.of(namespace, tableName)
    catalog.loadTable(tableID)
  }

  private def scanTableWithPartitionPredicate(table:Table, partitionPredicate:Expression):Try[TableScan] =
    Try(table.newScan.filter(partitionPredicate))

  private def getDataFilesLocations(tableScan:TableScan): Try[String] = Try {
    // chain all files to scan in a single string => "'file1', 'file2'"
    val scanFiles = tableScan.planFiles().asScala
      .map(f => "'" + f.file.path.toString + "'")
    scanFiles.foreach(f => logger.info(s"Got planned data file:$f"))
    scanFiles.mkString(",")
  }

  private def initDuckDBConnection: Try[Connection] = Try {
    val con = DriverManager.getConnection("jdbc:duckdb:")
    val init_statement =
      s"""
         |INSTALL httpfs;
         |LOAD httpfs;
         |SET s3_region='eu-west-1';
         |SET s3_access_key_id='${sys.env.get("AWS_ACCESS_KEY_ID").get}';
         |SET s3_secret_access_key='${sys.env.get("AWS_SECRET_ACCESS_KEY").get}';
         |SET s3_session_token='${sys.env.get("AWS_SESSION_TOKEN").get}';
         |""".stripMargin
    con.createStatement().execute(init_statement)
    con
  }

  private def executQuery(connection: Connection, query:String):Try[ResultSet] = Try{
     connection.createStatement.executeQuery(query)
  }

  private def formatQuery(query:String, dataFilesStr:String):Try[String]  = Try {
    query.replaceAll("<FILES_LIST>", dataFilesStr)
  }

  private def executeIcebergQuery(query:String): List[String] = {

    val partitionPredicate = Expressions.and(
      Expressions.equal("ltv_timestamp", "2022-03-01"),
      Expressions.equal("acc_id", "0123456"))

    val jsonDataRows = for {
      catalog         <- getGlueCatalog
      table           <- getIcebergTableByName("db_name", "table_name", catalog)
      tableScan       <- scanTableWithPartitionPredicate(table, partitionPredicate)
      dataFilesString <- getDataFilesLocations(tableScan)
      queryStatement  <- formatQuery(query, dataFilesString)
      dbConnection    <- initDuckDBConnection
      resultSet       <- executQuery(dbConnection, queryStatement)
    } yield resultSet.toStringList

    jsonDataRows match {
      case Success(jsonRows) => jsonRows
      case Failure(exception) => {
        logger.error("Error fetching data", exception)
        List[String]()
      }
    }

  }

  def main(args: Array[String]): Unit = {

    val query = """
         |SELECT row_to_json(lst)
         |FROM (
         | SELECT date, account_id, event_type, count(*) as events
         | FROM parquet_scan([ <FILES_LIST>])
         | WHERE acc_id = '0123456' and dt = '2022-03-01'
         | GROUP BY 1,2,3
         |
         |) lst
         |
         |""".stripMargin

    val results = executeIcebergQuery(query)
    results.foreach(result => logger.info(result))
  }


}
