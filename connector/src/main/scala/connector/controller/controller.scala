package connector.controller

import akka.actor._
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark._
import scala.io.Source

sealed abstract trait ControllerMessage 
case class StartSparkStream(host: String)
case class ReadFromFile(fname: String) extends ControllerMessage
case class SendGraph(g: Graph[Int, Int]) extends ControllerMessage
case object PrintComps extends ControllerMessage

class Controller(actSys: ActorSystem, sc: SparkContext) extends Actor {
	var graph: Option[Graph[Int, Int]] = None
	var streamContext: Option[StreamingContext] = None
	var edgeStream: Option[DStream[String]] = None
	
	private def buildGraphFromFile(fname: String) {
		val edgeBuilder = actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder")
		for(lines <- Source.fromFile(fname).getLines()) {
			val ids = lines.split(" ")
			val edge = Edge[Int](ids(0).toInt, ids(1).toInt)
			edgeBuilder ! AddEdge(edge)
		}
		edgeBuilder ! RequestGraph
	}
	
	def receive = {
		case StartSparkStream(host) => {
			streamContext = Some(new StreamingContext(sc, Seconds(1)))
			edgeStream = Some(streamContext.get.socketTextStream(host, 9999))
			
			val edgeBuilder = actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder")
			
			//edgeStream.flatMap(_.split(" "))

		}
		case ReadFromFile(fname) => buildGraphFromFile(fname)
		case SendGraph(g) => graph = Some(g); self ! PrintComps //TODO: This is a bit hack-y
		case PrintComps => {
			val cc = graph.get.connectedComponents().vertices
			println(cc.collect().mkString("\n"))
		}
	}
}