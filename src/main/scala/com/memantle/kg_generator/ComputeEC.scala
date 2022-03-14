package com.memantle.kg_generator

import com.esri.core.geometry.{OperatorTouches, Polygon, SpatialReference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import helpers.ESRIHelper
import scala.collection.mutable

object ComputeEC {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Compute EC using Polygons")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val indexDir = args(0)
    val regionsDir = args(1)
    val outputDir = args(2)
    val partitions = args(3).toInt
    val level = args(4).toInt

    val polygons = spark.read
      .parquet(regionsDir)
      .select($"fullid",$"gid",$"coords",$"name")




    val spatialIndexL = spark.read
      .parquet(indexDir)
      .select($"fullid", $"gid", $"token")


    val spatialIndexR = spatialIndexL
      .toDF(spatialIndexL.columns.map(_ + "_r"): _*)


    val candidates = spatialIndexL
      .join(spatialIndexR, $"token" === $"token_r" && $"gid" =!= $"gid_r" && $"fullid" < $"fullid_r")
      .select($"fullid",$"gid",$"fullid_r",$"gid_r")
      .distinct()
      .repartition(partitions)
      .cache()


    val polygonsB = spark.sparkContext
      .broadcast(
        polygons.rdd.map(x => {
          val gid = x(1).toString()
          val fullid = x(0).toString()
          val poly = ESRIHelper.createPolyNoSR(x(2).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]])
          (fullid,(gid,fullid,poly))
        }).collect().map(x => (x._1 -> x._2)).toMap)


    def touches = udf((fullid: String,fullid_r: String) => {
      val polygons = polygonsB.value
      val poly1 = polygons(fullid)._3
      val poly2 = polygons(fullid_r)._3
      OperatorTouches.local().execute(poly1, poly2, null, null)
    })


    val adjacentPolygons = candidates
      .filter(touches($"fullid",$"fullid_r"))
      .withColumn("rel",lit("EC"))
      .select($"gid",$"fullid",$"rel",$"gid_r",$"fullid_r")
      .distinct()
      .cache()

    val ecRelations = adjacentPolygons
      .select($"gid",lit("EC"),$"gid_r")

    ecRelations.write
      .mode("overwrite")
      .format("parquet")
      .save(outputDir+"full/level"+level)

  }
}