package com.memantle.kg_generator.parsers

import com.esri.core.geometry.SpatialReference
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.esri.core.geometry.SpatialReference
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/*
Takes GeoJSON input and converts to Parquet format.
A couple of important things:

1) GeoJSON encodes coordinates as longitude (x position) first.
Most GIS Applications expect latitude first
The following code stores as latitude first in parquet format

2) Regions can be polygons or multi-polygons.
For consistency the following codes outputs as either polygons or multipolygons.

3) The actual coordinates are stored as arrays

 */

object GADMParser {


  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Parse JSON Polys")

      .getOrCreate()




    val inputDir = args(0)
    val outputDir = args(1)
    val level = args(2).toInt
    val precision = args(3).toInt

    import org.apache.spark.sql.functions._
    import spark.implicits._


    var json  = spark.read.json(inputDir)

    var regions = json.select(
      col("properties.GID_"+level).as("regionid"),
      col("properties.NAME_"+level).as("name"),
      col("geometry.type").as("geomType"),
      col("geometry.coordinates").as("coords")
    )



    regions.cache()

    println("Total number of regions:"+regions.count())



    val normalisePolys = udf((loops: mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[String]]]) => {
      val polyArr = ArrayBuffer[ArrayBuffer[Array[Double]]]()
      for (i <- 0 until loops.length) {
        val loop = loops(i)
        val loopArr = ArrayBuffer[Array[Double]]()
        for(j<-0 until loop.length){

          val lng = BigDecimal(loop(j)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
          val lat = BigDecimal(loop(j)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
          //ESRI expects the x (lng) value first
          loopArr += Array(lng,lat)
        }
        polyArr += loopArr
      }
      polyArr
    })


    val polys = regions
      .filter($"geomType" === "Polygon")
      .withColumn("coords",normalisePolys(col("coords")))
      .withColumn("partid",lit(1))

    println("Polys:" + polys.count())
    polys.show()



    val normaliseMultiPolys = udf((loops: mutable.WrappedArray[mutable.WrappedArray[String]]) => {
      val polyArr = ArrayBuffer[ArrayBuffer[Array[Double]]]()
      for (i <- 0 until loops.length) {
        val loop = loops(i)
        val loopArr = ArrayBuffer[Array[Double]]()
        for(j<-0 until loop.length){

          val parts = loop(j).split(",")
          val lng = BigDecimal(parts(0).substring(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
          val lat = BigDecimal(parts(1).substring(0, parts(1).length - 1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
          //ESRI expects the x (lng) value first
          loopArr += Array(lng,lat)
        }
        polyArr += loopArr //we add loops to an array, at the end we get a polygon
      }
      polyArr
    })




    val multipolys = regions
      .filter($"geomType" === "MultiPolygon")
      .withColumn("coords",explode(col("coords")))
      .withColumn("coords", normaliseMultiPolys($"coords"))
      .withColumn("partid", row_number().over(Window.partitionBy(col("gid")).orderBy($"gid")))



    multipolys.show()
    //multipolys.filter($"gid" contains "BEL").show()

    println("MultiPolys count:" + multipolys.count())


    val unionOfRegions = polys.union(multipolys).withColumn("polygonid",concat($"regionid",lit("#"),$"partid"))


    //get count of the polygons for a region
    val byGid = Window.partitionBy('gid)


    val allRegions = unionOfRegions.withColumn("polycount", count('regionid) over byGid)



    println("Complete:")
    allRegions.show()


    //save
    allRegions.write.mode("overwrite").parquet(outputDir)




  }
}
