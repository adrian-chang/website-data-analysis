import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD

abstract class VertexProperty
case class WebsiteProperty(websiteId: String) extends VertexProperty
case class FontPackProperty(fontPack: String) extends VertexProperty
case class BusinessCategoryProperty(businessCategory: String) extends VertexProperty
case class ColorPackProperty(colorPack: String) extends VertexProperty

object Graphz {

  private var idMap: Map[String, Long] = Map()

  private var mapId: Map[Long, String] = Map()

  def buildGraph(websiteRDD: RDD[(Data.WebsiteId, Data.Layout, Data.ColorPack, Data.FontPack, Data.BusinessCategory)]) = {
    // need to get website vertexes

    val websiteVertexes = websiteRDD.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        (getId(id), WebsiteProperty(id).asInstanceOf[VertexProperty])
    }

    val businessCategoryVertexes = websiteRDD.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        businessCategory
    }.filter(_ != "").map {
      businessCategory =>
        (getId(businessCategory), BusinessCategoryProperty(businessCategory).asInstanceOf[VertexProperty])
    }

    val websiteBusinessEdges = websiteRDD.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        Edge(getId(id), getId(businessCategory), 1l)
    }

    val businessWebsiteEdges = websiteRDD.map {
      case (id, layout, colorPack, fontPack, businessCategory) =>
        Edge(getId(businessCategory), getId(id), 1l)
    }

    val graph = Graph[VertexProperty, Long](
      websiteVertexes.union(businessCategoryVertexes),
      websiteBusinessEdges.union(businessWebsiteEdges)).cache

    val result: Graph[Double, Double] = graph.staticPageRank(10)

    println()
    println("Pagerank results on graph")

    result.vertices.map {
      case (vertexId, rank) =>
        if (rank != 0.15) {
          (getName(vertexId), rank)
        } else {
          null
        }
    }.filter(_ != null).sortBy(_._2, false).take(10).foreach(println(_))
  }

  def getId(id: String): Long = {
    val num = idMap get id
    if (num.isEmpty) {
      val newId = idMap.size + 1l
      idMap = idMap + (id -> newId)
      mapId = mapId + (newId -> id)
      newId
    } else {
      num.get
    }
  }

  def getName(id: Long) = {
    mapId getOrElse(id, "None")
  }

}
