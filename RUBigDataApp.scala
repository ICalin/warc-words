package org.rubigdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup
import org.apache.spark.sql.functions.sum
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileSystem, Path}

object RUBigDataApp {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("RUBigDataApp").getOrCreate()
    import spark.implicits._
	
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileStatuses = fs.listStatus(new Path("hdfs:///single-warc-segment/"))
    val filePaths = fileStatuses.slice(0, 100).map(_.getPath.toString)

    var warcData = spark.read.textFile(filePaths(0))

    for (i <- 1 until filePaths.length) {
        warcData = warcData.union(spark.read.textFile(filePaths(i)))
    }

    // used to extract the references
    def extractUrls(content: String): List[String] = {
      val doc = Jsoup.parse(content)
      val links = doc.select("a[href]")
      links.asScala.map(_.absUrl("href")).toList
    }
    
    
    // check if a given domain appears in the links list 
    def containsReference(url: String, domain: String): Boolean = {
      url.toLowerCase.contains(domain.toLowerCase())
    }

    val references = warcData
      .flatMap { line =>
        val warcTypeIndex = line.indexOf("WARC-Type:")
        if (warcTypeIndex >= 0) {
          val warcType = line.substring(warcTypeIndex + 11).trim
          if (warcType == "response") {
            val httpStringBodyIndex = line.indexOf("\n\n")
            if (httpStringBodyIndex >= 0 && line.length > httpStringBodyIndex + 2) {
              val httpStringBody = line.substring(httpStringBodyIndex + 2)
              extractUrls(httpStringBody)
            } else {
              List.empty[String]
            }
          } else {
            List.empty[String]
          }
        } else {
          List.empty[String]
        }
      }

    val referenceCounts = references.map { url =>
      if (containsReference(url, "facebook")) "Facebook"
      else if (containsReference(url, "instagram")) "Instagram"
      else if (containsReference(url, "tiktok")) "Tiktok"
      else "other"
    }.groupBy("value").count()

    val socialMediaCounts = referenceCounts.filter($"value" =!= "other")
    val totalSocialMediaReferences = if (socialMediaCounts.isEmpty) {
        0L} else {
        socialMediaCounts.select(sum("count")).first().getLong(0)
    }

    socialMediaCounts.collect.foreach { row =>
      val website = row.getString(0)
      val count = row.getLong(1)
      val percentage = (count.toDouble / totalSocialMediaReferences) * 100
      val formattedPercentage = f"$percentage%.2f"
      println(s"$website references: $count ($formattedPercentage%)")
    }



    spark.stop()
  }
}

