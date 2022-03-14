package com.memantle.kg_generator.parsers

import org.apache.spark.sql.SparkSession

/**
  * Created by Matthew on 16/09/2021.
  */
object test {
  def main(args: Array[String]) {
        val spark = SparkSession
          .builder
          .appName("Parse JSON Polys")
          .config("spark.master", "local[4]")
          .getOrCreate()
    println("test")
  }
}
