package cse512

import org.apache.spark.sql.SparkSession

import scala.math.sqrt
import scala.math.pow

object SpatialQuery extends App{

  /**
   * Checks if point lies inside the given rectangle
   * @param queryRectangle Rectangle coordinates
   * @param queryPoint Point coordinates
   * @return True if point lies inside, otherwise False
   */
  def ST_Contains(queryRectangle : String, queryPoint : String) : Boolean = {

    // Validity checks
    if (Option(queryRectangle).getOrElse("").isEmpty
      || Option(queryPoint).getOrElse("").isEmpty) {
      return false
    }

    // Get Point -> (x,y)
    val point = queryPoint
      .split(',')
      .map(_.toDouble)
    val x = point(0)
    val y = point(1)

    // Find Rectangle Bounds
    val rectangle = queryRectangle
      .split(',')
      .map(_.toDouble)
    val upper_x = math.max(rectangle(0), rectangle(2))
    val lower_x = math.min(rectangle(0), rectangle(2))
    val upper_y = math.max(rectangle(1), rectangle(3))
    val lower_y = math.min(rectangle(1), rectangle(3))

    // Check if point is inside rectangle
    if (x > upper_x
      || x < lower_x
      || y > upper_y
      || y < lower_y) { false }
    else { true }
  }

  /**
   * Checks if two points are within the given limit of each other
   * @param queryPoint1 Point 1 coordinates
   * @param queryPoint2 Point 2 coordinates
   * @param queryDistance Rectangle coordinates
   * @return True is distance between Point 1 & 2 is less than or equal to queryDistance, otherwise False
   */
  def ST_Within(queryPoint1 : String, queryPoint2 : String, queryDistance : Double) : Boolean = {

    // Validity checks
    if (Option(queryPoint1).getOrElse("").isEmpty
      || Option(queryPoint2).getOrElse("").isEmpty
      || queryDistance <= 0) {
      return false
    }

    // Get Points -> (x,y)
    val point1 = queryPoint1.split(",")
    val x1 = point1(0).toDouble
    val y1 = point1(1).toDouble

    val point2 = queryPoint2.split(",")
    val x2 = point2(0).toDouble
    val y2 = point2(1).toDouble

    // Calculate euclidean distance
    val distance = sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))

    // Check if distance under limit
    if (distance <= queryDistance) { true }
    else { false }
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - Completed
    println("ST_Contains ---------->")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)
    =>((ST_Contains(queryRectangle, pointString))))
    val resultDf = spark.sql("SELECT * FROM point WHERE ST_Contains('"+arg2+"',point._c0)")

    println("<---------- ST_Contains ")
    resultDf.show()

    resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - Completed
    println("ST_Contains ---------->")

    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)
    =>((ST_Contains(queryRectangle, pointString))))
    val resultDf = spark.sql("SELECT * FROM rectangle,point WHERE ST_Contains(rectangle._c0,point._c0)")

    println("<---------- ST_Contains ")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - Completed
    println("ST_Within ---------->")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)
    =>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("SELECT * FROM point WHERE ST_Within(point._c0,'"+arg2+"',"+arg3+")")

    println("<---------- ST_Within ")
    resultDf.show()

    resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION - Completed
    println("ST_Within ---------->")

    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)
    =>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("SELECT * FROM point1 p1, point2 p2 WHERE ST_Within(p1._c0, p2._c0, "+arg3+")")

    println("<---------- ST_Within ")
    resultDf.show()

    resultDf.count()
  }
}
