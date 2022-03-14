package com.memantle.kg_generator


import com.esri.core.geometry.{OperatorContains, OperatorRelate, OperatorTouches, Point}
import helpers.ESRIHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object ComputeContain {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Compute point in region")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val pointsDir = args(0)
    val regionsDir = args(1)
    val outputDir = args(2)
    val partitions = args(3).toInt

    val convertGeom = udf((lat:Double,lng:Double)=>{
      Array(Array(Array(lat,lng)))
    })

    val cellsAndPoints = spark
      .read
      .parquet(pointsDir)
      .withColumn("intersection",convertGeom($"lat",$"lng"))
      .withColumn("type",lit("point"))
      .select($"id".as("gid"),$"type",$"intersection",$"token")


    val polygonCellIntersections = spark.read
      .parquet(regionsDir)
      .withColumn("type",lit("poly"))
      .select($"gid",$"type",$"coords".as("intersection"),$"token")



    val allGeoms = cellsAndPoints
      .union(polygonCellIntersections)
      .repartition(partitions,$"token")
      .cache()


    val allGeomsRDD = allGeoms.rdd.map(x=>{
      val gid = x(0).toString()
      val geomType = x(1).toString
      val intersection = x(2).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]]
      val token = x(3)
      (gid,geomType,intersection,token)
    })

    val allMatchingPointsAndRegions = allGeomsRDD.mapPartitionsWithIndex((index,iterator) => {
      val all = iterator.toList
      val regions = all.filter(x=>x._2 == "poly")
      val points = all.filter(x=>x._2 == "point")

      val regionsAndPoints = regions.map(region=>{
        val token = region._4
        val polygon = ESRIHelper.createPolyNoSR(region._3)
        val candidatePoints = points.filter(x=> x._4 == token)

        val matchingPoints = candidatePoints.filter(point=>{
          val lat = point._3(0)(0)(0)
          val lng = point._3(0)(0)(1)
          val esriPoint = new Point(lng, lat)
          OperatorContains.local().execute(polygon, esriPoint, null, null)
        }).map(matchingPoint=>(matchingPoint._1,region._1))
        matchingPoints
      }).flatMap(x=>x)
      Iterator(regionsAndPoints)
    }).flatMap(x=>x)

    //regionsAndPoints.cache()

    val regionsAndPointsDF = allMatchingPointsAndRegions.toDF("id","gid").cache()
    regionsAndPointsDF.show()
    println("Final count:"+regionsAndPointsDF.count)

    regionsAndPointsDF.write.format("parquet").mode("overwrite").save(outputDir)
  }
}
