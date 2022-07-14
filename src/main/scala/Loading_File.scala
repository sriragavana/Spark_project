package com.sr


import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.input_file_name
//import org.apache.log4j.{Logger,Level}
//import com.typesafe.config.ConfigFactory
import java.io.File





object Loading_File {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()


//    val consel = ConfigFactory.parseFile(new File("/dbfs/mnt/saacdcstructureduat_delta/sample1.conf"))
//    val selcustom =consel.getString("canonical.customer.value")

    val df5=spark.readStream.format("cloudFiles").option("header", true).option("sep", ",")
      .option("cloudFiles.schemaLocation","dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/financial_year_provisional_schema")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.format","csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year")


    val withMetadata = df5.withColumn("inputfilename",input_file_name()).selectExpr("*",s"current_timestamp() as spark_processing_time"
      ,"year(current_timestamp()) as spark_processing_year"
      ,"month(current_timestamp()) as spark_processing_month"
      ,"dayofmonth(current_timestamp()) as spark_processing_day")


    spark.sql("USE default")

    spark.sql("DROP TABLE IF EXISTS Financila_year_prov")

    withMetadata.writeStream.format("delta")
      .option("checkpointLocation","dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year_checkpoint")
      .option("path", "dbfs:/FileStore/shared_uploads/sriragavan.arumugam@aspiresys.com/Financial_year_outpath")
      .partitionBy("spark_processing_year","spark_processing_month","spark_processing_day")
      .toTable("Financila_year_prov")



  }

}
