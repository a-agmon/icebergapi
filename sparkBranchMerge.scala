  private def branchAndMerge(fullTableName: String)(implicit sparkSession: SparkSession) = {
    sparkSession.sql(s"ALTER TABLE $fullTableName  DROP BRANCH if exists branch02 ")

    sparkSession.sql(s"select count(*) from $fullTableName").show()
    //
    val tableRef = Spark3Util.loadIcebergTable(sparkSession, fullTableName)
    // create branch
    tableRef.manageSnapshots()
      .createBranch("branch02", tableRef.currentSnapshot().snapshotId())
      .commit()
    // read branch to df
    val branchDF = sparkSession.read
      .option("branch", "branch02")
      .table(fullTableName)

    // duplicate the data in the branch by writing to the branch
    branchDF
      .writeTo(s"$fullTableName.branch_branch02")
      .append()
    // view the branches
    sparkSession.sql(s"select * from $fullTableName.refs").show()
    sparkSession.sql(s"select count(*) from $fullTableName VERSION AS OF 'branch02'").show()

    sparkSession.sql(s"select count(*) from $fullTableName").show()
    // merging the branch
   tableRef.manageSnapshots()
      .fastForwardBranch(SnapshotRef.MAIN_BRANCH, "branch02")
      .commit()
    sparkSession.sql(s"select count(*) from $fullTableName").show()
  }
