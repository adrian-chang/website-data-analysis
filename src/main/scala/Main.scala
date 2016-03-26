import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sc = createContext
    val websiteRDD = Data.processWebsiteRDD(sc)
    Graphz.buildGraph(websiteRDD)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .setAppName(appName)
      .setMaster(masterUrl)

    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local[2]")

  def createContext: SparkContext = createContext("website-examine", "local[2]")

}
