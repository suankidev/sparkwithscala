package com.suanki
package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, collect_list, udf}

import scala.collection.mutable

object Joines {

  /**
   * Inner joins (keep rows with keys that exist in the left and right datasets)
   *
   * Outer joins (keep rows with keys in either the left or right datasets)
   *
   * Left outer joins (keep rows with keys in the left dataset)
   *
   * Right outer joins (keep rows with keys in the right dataset)
   *
   * Left semi joins (keep the rows in the left, and only the left, dataset where the key
   * appears in the right dataset)
   *
   * Left anti joins (keep the rows in the left, and only the left, dataset where they do not
   * appear in the right dataset)
   *
   * Natural joins (perform a join by implicitly matching the columns between the two
   * datasets with the same names)
   *
   * Cross (or Cartesian) joins (match every row in the left dataset with every row in the
   * right dataset)
   *
   * @param spark
   */


  def joins(spark: SparkSession) = {

    println("=" * 10 + "Start of the program" + "=" * 10)

    //import spark.implicits._
    val person = spark.createDataFrame(Seq(
      (0, "Bill Chambers", 0, List(100)),
      (1, "Matei Zaharia", 1, List(500, 250, 100)),
      (2, "Michael Armbrust", 1, List(250, 100)),
      (4, "Sujeet kumar singh", 6, List(100, 200, 250)),
      (4, "Vishal", 7, List(200, 250))
    )
    ).toDF("pid", "name", "graduate_program", "status")


    val graduateProgram = spark.createDataFrame(Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"),
      (6, "B.Texch", "School of Technology", "Sharda University"),
      (5, "M.Tech", "School of Engineering", "Jaypee institute of Technology")
    )
    ).toDF("gid", "degree", "department", "school")


    val sparkStatus = spark.createDataFrame(Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
    ).toDF("sid", "status")

    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")


    /**
     * """
     * person::
     * +---+----------------+----------------+---------------+
     * |pid |name            |graduate_program|spark_status   |
     * +---+----------------+----------------+---------------+
     * |0  |Bill Chambers   |0               |[100]          |
     * |1  |Matei Zaharia   |1               |[500, 250, 100]|
     * |2  |Michael Armbrust|1               |[250, 100]     |
     * +---+----------------+----------------+---------------+
     *
     * graduateProgram::
     * +---+-------+---------------------+-----------+
     * |gid |degree |department           |school     |
     * +---+-------+---------------------+-----------+
     * |0  |Masters|School of Information|Sharda University|
     * |2  |Masters|EECS                 |UC Berkeley|
     * |1  |Ph.D.  |EECS                 |UC Berkeley|
     * +---+-------+---------------------+-----------+
     *
     * sparkStatus::
     * +---+--------------+
     * |sid |status        |
     * +---+--------------+
     * |500|Vice President|
     * |250|PMC Member    |
     * |100|Contributor   |
     * +---+--------------+
     *
     */

    // person.show(false)
    //graduateProgram.show(false)
    //sparkStatus.show(false)


    /**
     * Whereas the join expression determines whether two rows should join, the join type determines
     * what should be in the result set.
     */

    import org.apache.spark.sql.functions.{col}
    //inner join
    var joinType = "inner"
    val innerJoinCondition = person.col("graduate_program") === graduateProgram.col("gid")

    val innerDf = person.join(graduateProgram, innerJoinCondition, joinType)

    print("->>>>>>>>>>>inner")
    // innerDf.show(truncate = false)

    spark.sql(
      """
        |select * from person inner join graduateprogram on person.graduate_program = graduateprogram.gid
        |""".stripMargin) //.show()


    joinType = "left_outer"

    println("->>>>>>>>>left_outer")
    //All the row from left table
    // and If there is no equivalent row in the right DataFrame, Spark will insert null for right row
    //no of output rows depends on left table
    val left_outer_Df = person.join(graduateProgram, innerJoinCondition, joinType)

    //left_outer_Df.show(false)


    println("->>>>>>>>>right_outer")
    joinType = "right_outer"
    val right_outer_Df = person.join(graduateProgram, innerJoinCondition, joinType)

    //right_outer_Df.show(false)

    //all the row from left table only , where it's matched with right table
    println("->>>>>>>>>left_semi")
    joinType = "left_semi"
    val left_semi_Df = person.join(graduateProgram, innerJoinCondition, joinType)
    //left_semi_Df.show(false)


    println("->>>>>>>>>left_anti")
    joinType = "left_anti"
    val left_anti_Df = person.join(graduateProgram, innerJoinCondition, joinType)
    //left_anti_Df.show(false)

    println("->>>>>>>>>cross")
    joinType = "cross"
    val cross_Df = person.join(graduateProgram, innerJoinCondition, joinType)
    //cross_Df.show(false)


    //complex type
    val cond = array_contains(person.col("status"), sparkStatus.col("sid"))


    val complex_df = person.join(sparkStatus, cond, "inner")

    complex_df.show(false)

    //logic to play with complex type

    val mylist = udf((myList: Any) => {

      val t = myList.asInstanceOf[mutable.WrappedArray[Int]]

      val j = t.toArray

      j
           // println(j.map(x=>x.toString))
      //val myArray=Array(100,250,500)
      //("100"->"Contributor","250"->"PMC Member","500"->"voice President")

    })

    val d = collect_list(person.col("status")(0))

    val finalDf = person.withColumn("finalcol", mylist(col("status")))

    finalDf.show(false)

    println("=" * 10 + "End of the program" + "=" * 10)


  }

}
