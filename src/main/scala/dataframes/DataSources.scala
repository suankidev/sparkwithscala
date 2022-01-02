package com.suanki
package dataframes

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SparkSession

import java.sql.DriverManager

object DataSources {

  def code(spark: SparkSession): Unit = {
    println("=" * 10 + "Start of the program" + "=" * 10)


    /**
     * Following are Spark’s core data
     * sources:
     * CSV
     * JSON
     * Parquet
     * ORC
     * JDBC/ODBC connections
     * Plain-text files
     *
     * As mentioned, Spark has numerous community-created data sources. Here’s just a small sample:
     * Cassandra
     * HBase
     * MongoDB
     * AWS Redshift
     * XML
     * And many, many others
     */


    /** Read Mode
     * permissive:::  Sets all fields to null when it encounters a corrupted record and places all corrupted records
     * in a string column called _corrupt_record
     *
     * dropMalformed ::: Drops the row that contains malformed records
     *
     * failFast ::::Fails immediately upon encountering malformed records
     *
     */

    import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType}
    val productSchema = StructType(
      Array(
        StructField("product", StringType, true),
        StructField("category", StringType, true),
        StructField("revenue", StringType, true)

      )
    )


    val df = spark.read.options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "sep" -> ",",
      "mode" -> "permissive",
      "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailWhiteSpace" -> "true",
      "quote" -> "["
    ))
      .schema(productSchema).csv("C:\\Users\\sujee\\Desktop\\productrevenu.csv")


    /**
     * PartitionBy, bucketBy, and sortBy work only for file-based data sources;
     *
     * Table 9-2. Spark’s save modes
     * Save mode Description
     * append Appends the output files to the list of files that already exist at that location
     * overwrite Will completely overwrite any data that already exists there
     * errorIfExists Throws an error and fails the write if data or files already exist at the specified location
     * ignore If data or files exist at the location, do nothing with the current DataF
     */

    df.show(false)

    df.printSchema()

    //    df.write.format("csv").mode(saveMode = "errorIfExists").save("src/main/resources/retail_data/")
    println("=" * 10 + "End of the program" + "=" * 10)


    /**
     * CSV stands for commma-separated values
     */


    //parquet
    /**
     * Parquet is an open source column-oriented data store that provides a variety of storage
     * optimizations, especially for analytics workloads.
     *
     * 1. It provides columnar compression, whicch saves storage space and allows for reading individual columns instead of entire files. It is a file
     * 2. support complex type
     *
     *
     */

    df.write.mode(saveMode = "overwrite").parquet("src/main/resources/retail_data/")

    /**
     * ORC Files
     * ORC is a self-describing, type-aware columnar file format designed for Hadoop workloads. It is
     * optimized for large streaming reads, but with integrated support for finding required rows
     * quickly.
     */




    def prodDf(table:String) = {
      spark.read
        .format("jdbc")
        .options(Map(
          "driver" -> "oracle.jdbc.driver.OracleDriver",
          //"url"->"jdbc:oracle:thin:@localhost:1521:PDBORCL",
          "url" -> "jdbc:oracle:thin:suanki/yourpassword@//localhost:1521/PDBORCL",
          "user" -> "suanki",
          "password" -> "lemktpr",
          "dbtable" -> s"${table}"
        )).load()

    }




    val d=DriverManager.getConnection("jdbc:oracle:thin:suanki/lemktpr@//localhost:1521/PDBORCL")
          println(d.isClosed)
           d.close()










  }

}
