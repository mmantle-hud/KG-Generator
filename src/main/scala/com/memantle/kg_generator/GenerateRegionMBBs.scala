package com.memantle.kg_generator

import com.esri.core.geometry.Envelope
import helpers.ESRIHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


object GenerateRegionMBBs {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Generate MBBs")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._


    val regionsDir = args(0)
    val outputDir = args(1)


    val regions = spark.read.parquet(regionsDir)


    regions.show()

    val getMBB = udf((coords:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]])=>{
      val esriPoly = ESRIHelper.createPolyNoSR(coords) //coords
      val env = new Envelope()
      esriPoly.queryEnvelope(env)
      val minLng = env.getUpperLeft().getX()
      val maxLat = env.getUpperLeft().getY()
      val maxLng = env.getLowerRight().getX()
      val minLat = env.getLowerRight().getY()
      Array(minLng,maxLat,maxLng, minLat)
    })

    val regionsRDD = regions
      .withColumn("mbr",getMBB($"coords"))
      .withColumn("minLng",$"mbr"(0))
      .withColumn("maxLat",$"mbr"(1))
      .withColumn("maxLng",$"mbr"(2))
      .withColumn("minLat",$"mbr"(3))
      .drop("mbr")
      .select($"regionid",$"minLng",$"maxLat",$"maxLng",$"minLat")
      .rdd
      .map(x=>{
        val regionid = x(0).toString()
        val minLng = x(1).asInstanceOf[Double]
        val maxLat = x(2).asInstanceOf[Double]
        val maxLng = x(3).asInstanceOf[Double]
        val minLat = x(4).asInstanceOf[Double]
        (regionid,(minLng,maxLat,maxLng,minLat))
      })

    val regionMBBs = regionsRDD.reduceByKey((a,b)=>{
      val minLng = Math.min(a._1,b._1)
      val maxLat = Math.max(a._2,b._2)
      val maxLng = Math.max(a._3,b._3)
      val minLat = Math.min(a._4,b._4)
      (minLng,maxLat,maxLng,minLat)
    })
      .map(x=>{
        val coords = x._2
        val minLng = coords._1
        val maxLat = coords._2
        val maxLng = coords._3
        val minLat = coords._4
        val arrCoords = Array(
          Array(
            Array(minLng,maxLat),
            Array(maxLng,maxLat),
            Array(maxLng,minLat),
            Array(minLng,minLat)
          )
        )
        (x._1,x._1,arrCoords,1)
      }).toDF("regionid","fullid","coords","polycount") // need to add dummy regionid and polycount properties for next stage

    println("MBBs count:"+regionMBBs.count)

    regionMBBs.show(false)

    regionMBBs.write.mode("overwrite").parquet(outputDir)


  }
}
