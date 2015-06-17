package connector.controller

import akka.actor._
import org.apache.spark.graphx._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark._
import scala.io.Source

sealed abstract trait ControllerMessage 
case class StartSparkStream(host: String) extends ControllerMessage
case class ReadFromFile(fname: String) extends ControllerMessage
case class SendGraph(g: Graph[Int, Int]) extends ControllerMessage
case object PrintComps extends ControllerMessage

/** An Akka actor that communicates with the Python side of the framework and directs
 *  the GraphX calculations on the Scala side. Uses a TCP/IP socket to communicate with
 *  Python and the Akka Actor system to communicate with other Scala objects.
 *  Receives messages that inherit from [[connector.controller.ControllerMessage]].
 *  
 *  @constructor Use a given ActorSystem & SparkContext.
 *  @param actSys The actor system
 *  @param sc The spark context 
 */
class Controller(actSys: ActorSystem, sc: SparkContext) extends Actor {
	private var graph: Option[Graph[Int, Int]] = None
	//TODO: Use TCP/IP socket instead of spark streaming.
	private var streamContext: Option[StreamingContext] = None
	private var edgeStream: Option[DStream[String]] = None
	
	/*** Implement the messages ***/
	private def startSparkStream(host: String) {
		streamContext = Some(new StreamingContext(sc, Seconds(1)))
		edgeStream = Some(streamContext.get.socketTextStream(host, 9999))
		
		val edgeBuilder = actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder")
	}
	private def buildGraphFromFile(fname: String) {
		val edgeBuilder = actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder")
		for(lines <- Source.fromFile(fname).getLines()) {
			val ids = lines.split(" ")
			val edge = Edge[Int](ids(0).toInt, ids(1).toInt)
			edgeBuilder ! AddEdge(edge)
		}
		edgeBuilder ! RequestGraph
	}
	private def sendGraph(g: Graph[Int, Int]) {
		graph = Some(g); self ! PrintComps //TODO: This is a bit hack-y
	}
	private def printComps {
		val cc = graph.get.connectedComponents().vertices
		println(cc.collect().mkString("\n"))
	}
	
	def receive = {
		case StartSparkStream(host) => startSparkStream(host)
		case ReadFromFile(fname) => buildGraphFromFile(fname)
		case SendGraph(g) => sendGraph(g)
		case PrintComps => printComps
	}
}