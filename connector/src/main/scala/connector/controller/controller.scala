package connector.controller

import akka.actor._
import org.apache.spark.graphx._
import org.apache.spark._
import scala.io.Source
import java.net.Socket
import java.io.{BufferedReader, PrintWriter, InputStreamReader}

sealed abstract trait ControllerMessage 
case class StartListening(socket: Socket) extends ControllerMessage
case class ReadFromFile(fname: String) extends ControllerMessage
case class SendGraph(g: Graph[Nothing, Nothing]) extends ControllerMessage
case object PrintComps extends 	ControllerMessage
case object ShutdownController extends ControllerMessage

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
	private var graph: Option[Graph[Nothing, Nothing]] = None
	private var edgeBuilder: Option[ActorRef] = None
	private var socket: Option[Socket] = None
	private var in: Option[BufferedReader] = None
	private var out: Option[PrintWriter] = None
	
	private var stopListening = false
	
	private def listen() {
		var inputLine = ""
		stopListening = false
		while((inputLine = in.get.readLine) != null && !stopListening) {
			// Parse the message.
			val message = PyScMessage.parse(inputLine)
			
			// React.
			message match {
				case ListenForEdges => {
					// Initialize a new edge builder.
					
					edgeBuilder = Some(actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder"))
				} case FinishedMapping => {
					stopListening = true
					//TODO: Start calculating components
				} case Shutdown => {
					stopListening = true
					self ! ShutdownController
				} case DataMessage(edges) => {
					edgeBuilder.get ! edges
				}
			}
		}
	}
	
	/*** Implement the messages ***/
	private def startListening(newSocket: Socket) {
		// Store the socket.
		socket = Some(newSocket)
		
		// Prepare to listen.
		//TODO: Wrap in try-catch?
		in = Some(new BufferedReader(new InputStreamReader(socket.get.getInputStream)))
		out = Some(new PrintWriter(socket.get.getOutputStream))
		
		// Listen
		listen()
	}
	private def buildGraphFromFile(fname: String) {
		val edgeBuilder = actSys.actorOf(Props(new EdgeRddBuilder(sc)), "edgebuilder")
		for(lines <- Source.fromFile(fname).getLines()) {
			val ids = lines.split(" ")
			val edge = Edge(ids(0).toInt, ids(1).toInt)
			edgeBuilder ! AddEdge(edge)
		}
		edgeBuilder ! RequestGraph
	}
	private def sendGraph(g: Graph[Nothing, Nothing]) {
		graph = Some(g); self ! PrintComps //TODO: This is a bit hack-y
	}
	private def printComps {
		//TODO: Is there a better way to get the CC's of a graph of nothing's?
		val cc = graph.get.asInstanceOf[Graph[Int, Int]].connectedComponents.vertices
		println(cc.collect().mkString("\n"))
	}
	
	def receive = {
		case StartListening(newSocket) => startListening(newSocket)
		case ReadFromFile(fname) => buildGraphFromFile(fname)
		case SendGraph(g) => sendGraph(g)
		case PrintComps => printComps
		case ShutdownController => //TODO: Implement this
	}
}