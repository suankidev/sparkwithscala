package com.suanki
package rdds

import org.apache.spark.sql.SparkSession

object Rdds {


  def rdds(spark: SparkSession) = {

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Spark Simple".split(" ")

    val words = spark.sparkContext.parallelize(myCollection, 2)

    words.setName("myWords")

    val textRdd = spark.sparkContext
      .textFile("C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy\\data\\fligt_data\\")


    val rng = spark.range(10)
    val r = rng.toDF("id").rdd.map(f => f.getLong(0))

    //    r.foreach(println)

    words.distinct().count()

    def check(x: String) = {
      x.startsWith("S")
    }
    //    words.filter(f=>check(f)).foreach(println)


    //    words.map(f=>{if(f.startsWith("S")) f.toUpperCase()
    //    else
    //      f.toLowerCase()
    //    }).foreach(println)


    def myfun(x: String): String = {
      var y = x.toLowerCase()
      if (y.contains("data")) y
      else " "
    }

    words.map(f => {
      var word = myfun(f)
      if (word != " ") {
        word
      }
    })

    println()

  }
}
