package connector.controller

import akka.actor.{Actor, ActorRef}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl

sealed abstract trait EdgeRddBuilderMessage
case object Reset extends EdgeRddBuilderMessage
case class AddEdge[Int](edge: Edge[Int]) extends EdgeRddBuilderMessage
case object RequestGraph extends EdgeRddBuilderMessage

/** An Akka actor that builds an EdgeRDD from edge objects.
 *  Should be instantiated using an ActorSystem, not by calling a constructor.
 *  Receives messages that inherit from [[connector.controller.EdgeRddBuilderMessage]].
 *  
 *  @constructor Build the RDD in a specific SparkContext.
 *  @param sc The spark context 
 */
class EdgeRddBuilder(sc: SparkContext) extends Actor {
	private var edges: EdgeRDD[Int] = newEdgeRDD

	/** Constructs an empty EdgeRDD. */
	private def newEdgeRDD() = EdgeRDD.fromEdges(sc.parallelize(Array[Edge[Int]]()))
	
	/*** Implement the messages ***/
	private def reset() { 
		edges = newEdgeRDD 
	}
	private def addEdge(edge: Edge[Any]) {
		//TODO: Find some way to avoid this horrible type casting?
		val newRdd: EdgeRDD[Int] = EdgeRDD.fromEdges[Int, Int](sc.parallelize(List(edge.asInstanceOf[Edge[Int]])))
		edges = EdgeRDD.fromEdges(edges.union(newRdd))
	}
	private def requestGraph(sender: ActorRef) {
		val verts = VertexRDD.fromEdges(edges, 1, 0)
		val graph = GraphImpl.fromExistingRDDs(verts, edges)
		sender ! SendGraph(graph)
	}

	def receive = {
		case Reset => edges = newEdgeRDD
		case AddEdge(edge) => addEdge(edge)
		case RequestGraph => requestGraph(sender)
	}
}
