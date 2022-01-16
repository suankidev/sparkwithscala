package com.suanki
package rdds

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.Row

object Rdds {

  /**
   * Manuplating Rdds
   *
   *   1. Transformation
   *      distinct
   *      filter          #should return boolean for whatever row is sent
   *      map            //take one element from rdd and perform businees logic and return one element but flatMap return 1,0 or more element(flattern out the row)
   *      flatMap:--
   *      flatMap provides a simple extension of the map function we just looked at. Sometimes, each
   *      current row should return multiple rows, instead. For example, you might want to take your set
   *      of words and flatMap it into a set of characters
   *
   * sortBy
   */

  /** 2. Action
   * reduce::::::-
   * You can use the reduce method to specify a function to “reduce” an RDD of any kind of value
   * to one value
   *
   * count
   * countApprox
   * min
   * max
   * take
   * takeSample
   *
   *
   */

  /**
   * With cache(), you use only the default storage level :
   * MEMORY_ONLY for RDD
   * MEMORY_AND_DISK for Dataset
   *
   * With persist(), you can specify which storage level you want for both RDD and Dataset.
   * From the official docs:
   * You can mark an RDD to be persisted using the persist() or cache() methods on it.
   * each persisted RDD can be stored using a different storage level
   * The cache() method is a shorthand for using the default storage level, which is StorageLevel.MEMORY_ONLY (store deserialized objects in memory).
   *
   * Use persist() if you want to assign a storage level other than :
   * MEMORY_ONLY to the RDD
   * or MEMORY_AND_DISK for Dataset
   *
   *
   */
  //words.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY_2)

  //checkpointing , which is not available to dataframe api

  /**
   * Checkpointing
   * is the act of saving an RDD to disk so that future references to this RDD point to those
   * intermediate partitions on disk rather than recomputing the RDD from its original source. This is
   * similar to caching except that it’s not stored in memory, only disk.
   */

  def rdds(spark: SparkSession, mydf: (Dataset[Row], Dataset[Row])) = {

    val myCollection = "Spark The Definitive Guide : Big Data Processing Made Spark Simple".split(" ")

    val words = spark.sparkContext.parallelize(myCollection, 2) //Array(String)

    words.setName("myWords")

    val textRdd = spark.sparkContext.textFile("C:\\Users\\sujee\\OneDrive\\Documents\\bigdata_and_hadoop\\databricks_udemy\\data\\fligt_data\\2010-summary.csv")
    //will create Array(string)

    val rng = spark.range(10) //Dataset[Long]
    val r = rng.toDF("id").rdd.map(f => f.getLong(0))

    val total_count = words.distinct().count()

    println(s"total no of unique word are ${total_count}")


    def check(x: String) = {
      x.startsWith("S")
    }

    val words2 = words.filter(f => check(f))

    words.map(f => (f, f(0), check(f)))
      .filter(f => f._3)





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
    val textRdd1 = textRdd.map(f => f.split(","))
    val header = textRdd1.first()
    val textRdd2 = textRdd1.filter(f => f(0) != header(0))


    val sorted_w = words.sortBy(_.length)


    val r1 = spark.sparkContext.parallelize(1 to 20).reduce((x, y) => x + y)

    //finding the longest word in words rdd

    def longest_word(x: String, y: String): String = {
      if (x.length > y.length) x
      else y
    }


    val longest_w = words.reduce(longest_word)
    val longest_w1 = words.reduce((x, y) => longest_word(x, y))

    //since both processsing and definitive are same lenght may get sometime one of them
    println(longest_w + " " + longest_w1)

    words.countApprox(200, confidence = 0.95)

    words.countApproxDistinct(0.5)
    //Q2.      How many times data came in words rdd


    words.map(f => (f, 1))

    words.takeSample(false, 5, 2)


    //To save a textFile

    //words.saveAsTextFile("path")

    println(s"storage level ${words.getStorageLevel} \n number of partition ${words.getNumPartitions}")


    //    r.foreach(println)

    //checkpointing

    spark.sparkContext.setCheckpointDir("C:\\Users\\sujee\\AppData\\Local\\Temp")
    words2.checkpoint()
    //Now, when we reference this RDD, it will derive from the checkpoint instead of the source data.
    //This can be a helpful optimization


    //pipe to execute linux command
    words.pipe("wc -l").collect()

    println()

  }
}
