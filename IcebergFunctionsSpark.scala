import java.time.LocalDateTime
import java.time.LocalDate

object IcebergFunctions {

  def getRewriteDataFilesProc(fullTableName:String, icebergCatalog:String, targetFileSizeMB:Int): String = {
    val bytes = targetFileSizeMB * 1000 * 1000
    s"""
       |CALL $icebergCatalog.system.rewrite_data_files(table => '$fullTableName', strategy => 'sort',
       | sort_order => 'type DESC NULLS LAST, source DESC NULLS LAST, insts DESC NULLS LAST',
       | options => map (
       |    'min-input-files','15'
       |    ,'partial-progress.enabled','true'
       |    ,'max-concurrent-file-group-rewrites','96'
       |    ,'target-file-size-bytes','$bytes'
       |    )
       | )
       |""".stripMargin
  }
  def getRewriteDataFilesWhereProc(fullTableName:String, icebergCatalog:String, targetFileSizeMB:Int, fieldName:String, fieldStrVal:String): String = {
    val bytes = targetFileSizeMB * 1000 * 1000
    s"""
       |CALL $icebergCatalog.system.rewrite_data_files(table => '$fullTableName', strategy => 'sort',
       | sort_order => 'type DESC NULLS LAST, source DESC NULLS LAST, insts DESC NULLS LAST',
       | options => map (
       |    'min-input-files','15'
       |    ,'partial-progress.enabled','true'
       |    ,'max-concurrent-file-group-rewrites','96'
       |    ,'target-file-size-bytes','$bytes'
       |    ),
       | where => '$fieldName = "$fieldStrVal"'
       | )
       |""".stripMargin
  }
  def getExpireSnapshotsProc(fullTableName:String, icebergCatalog:String): String = {
    s"""
       |CALL $icebergCatalog.system.expire_snapshots(table =>'$fullTableName', older_than => TIMESTAMP '${LocalDate.now().toString}',
       | max_concurrent_deletes => 16, stream_results => true)
       |""".stripMargin
  }
  def getRewriteManifestProc(fullTableName:String, icebergCatalog:String): String = {
    s"""
       |CALL $icebergCatalog.system.rewrite_manifests('$fullTableName', false)
       |""".stripMargin
  }
  def getRemoveOrphanFilesProc(fullTableName:String, icebergCatalog:String, maxConcurDeletes:Int): String = {
    s"""
       |CALL $icebergCatalog.system.remove_orphan_files(table => '$fullTableName', max_concurrent_deletes => $maxConcurDeletes)
       |""".stripMargin
  }
}
