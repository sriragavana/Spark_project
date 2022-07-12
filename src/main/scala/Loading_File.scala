package com.sr

import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.spark.sql.SparkSession
//import com.typesafe.config.ConfigFactory
import java.io.File

object Loading_File {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


//    val consel = ConfigFactory.parseFile(new File("/dbfs/mnt/saacdcstructureduat_delta/sample1.conf"))
//    val selcustom =consel.getString("canonical.customer.value")

    val df5=spark.readStream.format("cloudFiles").option("header", true).option("sep", ",")
      .option("cloudFiles.schemaLocation","dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/financial_year_provisional_schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.format","csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year")


    spark.sql("USE default")

    spark.sql("DROP TABLE IF EXISTS Financila_year_prov")

    df5.writeStream.format("delta")
      .option("checkpointLocation","dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year_checkpoint")
      .option("path", "dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year_outpath")
      .toTable("Financila_year_prov")


  }

}
