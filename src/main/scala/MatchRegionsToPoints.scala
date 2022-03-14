import com.esri.core.geometry.{Envelope, OperatorContains, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import info.debatty.java.stringsimilarity.Jaccard
import org.apache.spark.sql.functions._

/**
  * Created by Matthew on 14/03/2022.
  */
object MatchRegionsToPoints {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Link YAGO to GADM")
      //      .config("spark.master", "local[4]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._


    val pointsDir = args(0)
    val mbrsDir = args(1)
    val geomsDir = args(2)
    val typesDir = args(3)
    val typesStr = args(4)
    val outputDir = args(6)
    val partitions = args(7).toInt
    val threshold = args(8).toDouble
    val words = args(9)


    val wordsArr = words.split("%")

    val getShortLabel = udf((label:String)=> {
      val shortLabel = label.substring(1, label.indexOf("@")-1)
      val normalLabel = if(shortLabel.lastIndexOf(",") > -1 )
        shortLabel.substring(0,shortLabel.lastIndexOf(","))
      else{
        shortLabel
      }
      normalLabel
    })

    val points = spark.read.parquet(pointsDir).withColumn("label",getShortLabel($"label"))

    points.select($"id",$"label").show(false)

    val typesArr = typesStr.split("%").map(t=>"<"+t+">")

    val types = spark.read.parquet(typesDir).filter($"object".isin(typesArr:_*))


    val filteredPoints = points.join(types,$"id"===$"subject")
      .drop("subject","object")
      .cache()


    val mbrs = spark.read.parquet(mbrsDir)
      .select($"gid",$"name",$"mbr",$"token".as("token_m"))


    val mbrContainsPoint = udf((lat:Double,lng:Double,mbr:Array[Array[Double]])=>{
      val esriPoint = new Point(lng, lat)
      val minX = mbr(1)(1)
      val minY = mbr(1)(0)
      val maxX = mbr(3)(1)
      val maxY = mbr(3)(0)
      val env = new Envelope(minX,minY,maxX,maxY)
      OperatorContains.local().execute(env, esriPoint, null, null)

    })

    val candidates = filteredPoints
      .join(mbrs,$"token"===$"token_m")
      .filter(mbrContainsPoint($"lat",$"lng",$"mbr"))



    val getScore = udf((label:String, name:String)=>{
      val jw = new Jaccard()
      var shortLabel = label.toLowerCase()
      var nameLower = name.toLowerCase()//.replaceAll(" ","")
      for(word <- wordsArr) {
        shortLabel = shortLabel.replaceAll(word,"")
        nameLower = nameLower.replaceAll(word,"")
      }
      jw.similarity(nameLower,shortLabel)
    })

    val candidatesWithScore =  candidates
      .withColumn("score",getScore($"label",$"name"))
      .select($"id",$"gid",$"name",$"label",$"score",$"mbr")

    val windowSpec = Window.partitionBy("gid")

    val matchingPoints = candidatesWithScore.withColumn("newscore", max("score") over windowSpec)
      .filter($"newscore" === $"score")
      .groupBy($"gid")
      .agg(
        first($"id").as("id"),
        first($"label").as("label"),
        first($"name").as("name"),
        first($"newscore").as("score")
      )

    matchingPoints.show(false)

    matchingPoints
      .write
      .format("parquet").mode("overwrite").save(outputDir)


  }
}
