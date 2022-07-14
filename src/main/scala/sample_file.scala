package com.sr
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.sql.Date
import org.apache.log4j.{Level, Logger}

object sample_file  {

  def main(args: Array[String]): Unit = {

Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().master("local").getOrCreate()

    val schema = StructType(Array(

      StructField("AirportCode", IntegerType, false),
      StructField("Date", DateType, false),
      StructField("TempHighF", IntegerType, false),
      StructField("TempLowF", IntegerType, false)
    ))

    val data = List(
      Row(11, Date.valueOf("2021-04-03"), 52, 43),
      Row(22, Date.valueOf("2021-04-02"), 50, 38),
      Row(13, Date.valueOf("2021-04-01"), 52, 41),
      Row(14, Date.valueOf("2021-04-03"), 64, 45),
      Row(15, Date.valueOf("2021-04-02"), 61, 41),
      Row(16, Date.valueOf("2021-04-01"), 66, 39),
      Row(17, Date.valueOf("2021-04-03"), 57, 43),
      Row(18, Date.valueOf("2021-04-02"), 54, 39),
      Row(19, Date.valueOf("2021-04-01"), 56, 41)
    )

    val rdd = spark.sparkContext.makeRDD(data)

    val tempRdd = spark.createDataFrame(rdd, schema)

    spark.sql("USE default")

    spark.sql("DROP TABLE IF EXISTS demo_temps_table")
    tempRdd.write.saveAsTable("demo_temps_table")

    print("Heloo!!!!")

    val df_temps = spark.sql("SELECT * FROM demo_temps_table ")
    tempRdd.show()
  }
}
