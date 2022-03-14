package com.memantle.kg_generator.helpers

import com.esri.core.geometry.{OperatorSimplifyOGC, Polygon, SpatialReference, _}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Matthew on 24/06/2021.
  */
object ESRIHelper {

  def createPoly(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]],sr:SpatialReference):Polygon={
    val poly = new Polygon()
    for(i<-0 until loops.size){
      val loop = loops(i)
      val x = loop(0)(0)
      val y = loop(0)(1)
      poly.startPath(x,y)
      for(j<-1 until loop.size){
        val x = loop(j)(0)
        val y = loop(j)(1)
        poly.lineTo(x,y)
      }
    }

    val g = OperatorSimplifyOGC.local().execute(poly, sr, true, null)
    g.asInstanceOf[Polygon]
  }
  def createPolyNoSR(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]):Polygon={
    val poly = new Polygon()
    for(i<-0 until loops.size){
      val loop = loops(i)
      val x = loop(0)(0)
      val y = loop(0)(1)
      poly.startPath(x,y)
      for(j<-1 until loop.size){
        val x = loop(j)(0)
        val y = loop(j)(1)
        poly.lineTo(x,y)
      }
    }

    val g = OperatorSimplifyOGC.local().execute(poly, null, true, null)
    g.asInstanceOf[Polygon]
  }

  /*
  def createMultiPoly(coords:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]],sr:SpatialReference):Polygon={
    val poly = new Polygon()
    for(i<-0 until coords.size){
      val loops = coords(i)
      for(j<-0 until loops.size){
        val loop = loops(j)
        val x = loop(0)(0)
        val y = loop(0)(1)
        poly.startPath(x,y)
        for(k<-1 until loop.size){
          val x = loop(k)(0)
          val y = loop(k)(1)
          poly.lineTo(x,y)
        }
      }
    }

    val g = OperatorSimplifyOGC.local().execute(poly, sr, true, null)
    g.asInstanceOf[Polygon]
  }
*/
  def createPolyFromS2Cell(coords:Array[Array[Double]]):Polygon={
    val precision = 5
    val poly = new Polygon()
    val x = BigDecimal(coords(0)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
    val y = BigDecimal(coords(0)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
    poly.startPath(x,y)
    for(j<-1 until coords.size){
      val x = BigDecimal(coords(j)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
      val y = BigDecimal(coords(j)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
      poly.lineTo(x,y)
    }
    val g = OperatorSimplifyOGC.local().execute(poly,null, true, null)
    g.asInstanceOf[Polygon]
  }
  def convertGeometryToArray(geometry:Polygon) : String={
    val jsonString = OperatorExportToJson.local().execute(null, geometry)
    jsonString
  }

  def flipCoords(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]):
  ArrayBuffer[ArrayBuffer[Array[Double]]]={
    val polyArr = ArrayBuffer[ArrayBuffer[Array[Double]]]()
    for (i <- 0 until loops.length) {
      val loop = loops(i)
      val loopArr = ArrayBuffer[Array[Double]]()
      for(j<-0 until loop.length){
        val lng = loop(j)(0)
        val lat = loop(j)(1)
        //flip the coords
        loopArr += Array(lat,lng)
        // coordsArr [[10,40]] -> [[10,40],[23,45]] at the end we get a loop
      }
      polyArr += loopArr //we add loops to an array, at the end we get a polygon
    }
    polyArr
  }//end of flip coords
}