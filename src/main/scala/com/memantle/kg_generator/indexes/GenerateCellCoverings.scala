package com.memantle.kg_generator.indexes


  import java.util
  import scala.collection.JavaConverters._
  import com.esri.core.geometry._
  import com.google.common.geometry._
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.{SparkConf, SparkContext}
  import com.memantle.kg_generator.helpers.ESRIHelper
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.storage.StorageLevel

  import scala.collection.mutable
  import scala.collection.mutable.{ArrayBuffer, ListBuffer}

  object GenerateCellCoverings {

    def getCoordsFromCellId(cellId:String): Array[Array[Double]] ={
      val s2Cell = new S2Cell(S2CellId.fromToken(cellId))
      val point = new S2LatLng(s2Cell.getVertex(0))
      val lat = point.latDegrees()
      val lng = point.lngDegrees()
      val cellCoordsArray = ArrayBuffer[Array[Double]]()
      cellCoordsArray += Array(lat, lng)
      for (i <- 1 to 3) {
        val point = new S2LatLng(s2Cell.getVertex(i))
        val lat = point.latDegrees()
        val lng = point.lngDegrees()
        //println(lng+":"+lat+":"+point.isValid)
        cellCoordsArray += Array(lat, lng)
      }
      cellCoordsArray.toArray
    }

    def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("Generate cell coverings and polygon-cell intersections")
        .getOrCreate()


      import spark.implicits._
      import org.apache.spark.sql.functions._



       val regionsDir = args(0)
       val intersectionOutputDir = args(1)
       val indexOutputDir = args(2)
       val partitions = args(3).toInt
       val s2level = args(4).toInt
       val includeIntersections = args(5).toBoolean

       val regions = spark.read
        .parquet(regionsDir)
        .select($"gid", $"fullid", $"coords", $"count")
        .repartition(partitions)
        .cache()



      val getCellCoverings = udf((loops: mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]) => {

        //var loopList = new ListBuffer[S2Loop]()
        val jList = new util.ArrayList[S2Loop]()
        for (i <- 0 until loops.length) {
          val loop = loops(i)
          val points = new ListBuffer[S2Point]
          for (j <- 0 until loop.length - 1) {
            //s2 expects Latitude first
            val point = S2LatLng.fromDegrees(loop(j)(1), loop(j)(0)).toPoint()
            points += point
          }
          val pointsList = points.toList.asJava
          val s2Loop = new S2Loop(pointsList)
          s2Loop.normalize()
          jList.add(s2Loop)
        }

        val s2polygon = new S2Polygon()

        s2polygon.init(jList)
        val initialCellIds = new util.ArrayList[S2CellId]()
        val regionCoverer = new S2RegionCoverer()
        regionCoverer.setMinLevel(s2level)
        regionCoverer.setMaxLevel(s2level)
        //  regionCoverer.setMaxCells(15)
        val covering = regionCoverer.getCovering(s2polygon)
        //  val cellIds = covering.cellIds().toArray
        covering.denormalize(s2level, 1, initialCellIds)
        val cellIds = initialCellIds.toArray()
        val tokens = cellIds.map(cellId => {
          val s2CellId = cellId.asInstanceOf[S2CellId]
          val token = s2CellId.toToken()
          token
        })
        tokens
      })

      val regionsWithTokens = regions.withColumn("tokens", getCellCoverings($"coords"))

      val cellCoverings = regionsWithTokens
        .drop($"coords")
        .withColumn("token", explode(col("tokens")))
        .select($"token", $"gid", $"fullid", $"count")
        .repartition(partitions)
        .cache()


      cellCoverings.write
        .mode("overwrite")
        .format("parquet")
        .save(indexOutputDir)


      if (includeIntersections) {

        val regionsMap = regions.rdd.map(x => {
          val gid = x(0).toString()
          val fullid = x(1).toString()
          val coords = x(2).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]]
          (fullid, (gid, fullid, coords))
        }).collect().map(x => (x._1 -> x._2)).toMap



        val regionsMapB = spark.sparkContext.broadcast(regionsMap)

        regions.unpersist()

        val jsonSchema = StructType(Array(
          StructField("rings", ArrayType(ArrayType(ArrayType(DoubleType)))),
          StructField("spatialReference", StructType(Array(StructField("wkid", IntegerType))))
        ))



        val getIntersection = udf((
                                    token: String,
                                    fullid: String
                                  ) => {

          val polyCoords = regionsMapB.value(fullid)._3
          val esriPoly = ESRIHelper.createPolyNoSR(polyCoords) //coords

          val s2Cell = new S2Cell(S2CellId.fromToken(token))
          val point = new S2LatLng(s2Cell.getVertex(0))
          val lat = point.latDegrees()
          val lng = point.lngDegrees()
          val cellCoordsArray = getCoordsFromCellId(token)

          val cellPoly = ESRIHelper.createPolyFromS2Cell(cellCoordsArray)

          val inputGeoms = new util.ArrayList[Geometry]()
          inputGeoms.add(cellPoly)
          val inGeoms = new SimpleGeometryCursor(inputGeoms)
          val intersector = new SimpleGeometryCursor(esriPoly)
          val outGeoms = OperatorIntersection.local().execute(inGeoms, intersector, null, null, -1)
          val esriResult = outGeoms.next().asInstanceOf[Polygon]
          val esriJSON = ESRIHelper.convertGeometryToArray(esriResult)
          esriJSON
        })

        val intersections = cellCoverings
          .withColumn("json", getIntersection($"token", $"fullid"))
          .cache()



        intersections
          .withColumn("json-unpacked", from_json($"json", jsonSchema))
          .withColumn("coords", $"json-unpacked.rings")
          .select($"token", $"gid", $"fullid", $"coords")
          .write
          .mode("overwrite")
          .format("parquet")
          .save(intersectionOutputDir)


      } //end of include intersections

    }
}
