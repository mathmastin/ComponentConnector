package connector

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl._
import org.apache.spark.rdd.RDD

object MyApp {
  def main(args: Array[String]) {
    // Create the spark context
    val conf = new SparkConf()
      .setMaster("local[2]")
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext(conf.setAppName("TestApp"))
    // Load the graph
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Find the cc's
    val cc = graph.connectedComponents().vertices
    // Join the cc's with usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
    // Stop the context
    sc.stop()
  }
}
