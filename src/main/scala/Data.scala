import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * All of the data processing and churn!
  */
object Data {

  type WebsiteId = String
  type Layout = String
  type ColorPack = String
  type FontPack = String
  type BusinessCategory = String

  def processWebsiteRDD(sc: SparkContext): RDD[(WebsiteId, Layout, ColorPack, FontPack, BusinessCategory)] = {
    val website = sc.cassandraTable("websitebuilder_v8", "website")
    val websites: RDD[(WebsiteId, Layout, ColorPack, FontPack, BusinessCategory)] = website.map {
      case (row) =>
        val props = row.getMap[String, String]("properties")
        val id = row.getString("website_id")

        if (props.nonEmpty) {
          (
            id,
            props getOrElse("layout", ""),
            props getOrElse("colorPack", ""),
            props getOrElse("fontPack", ""),
            props getOrElse("businessCategory", "")
          )
        } else {
          null
        }
    }.filter(_ != null).cache

    val colorPacks = websites.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        (colorPack, 1)
    }.reduceByKey(_ + _).sortBy(_._2, false).cache

    println("Top 10 color packs")
    colorPacks.take(10).foreach(println(_))
    println()

    val fontPacks = websites.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        (fontPack, 1)
    }.reduceByKey(_ + _).sortBy(_._2, false).cache

    println("Top 10 font packs")
    fontPacks.take(10).foreach(println(_))
    println()

    val businessCategories = websites.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        (businessCategory, 1)
    }.reduceByKey(_ + _).sortBy(_._2, false).cache

    println("Top 10 businessCategories")
    businessCategories.take(10).foreach(println(_))
    println()

    val layouts = websites.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        (layout, 1)
    }.reduceByKey(_ + _).cache

    println(s"Total amount of websites = ${websites.count}")
    println(s"Total amount of layouts = ${layouts.count}")
    println(s"Total amount of businessCategories = ${businessCategories.count}")
    println(s"Total amount of fontPacks = ${fontPacks.count}")
    println(s"Total amount of colorPacks = ${colorPacks.count}")

    websites
  }

}
