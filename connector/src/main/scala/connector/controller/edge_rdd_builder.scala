package connector.controller

import akka.actor.Actor
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl

sealed abstract trait EdgeRddBuilderMessage
case object Reset extends EdgeRddBuilderMessage
case class AddEdge[Int](edge: Edge[Int]) extends EdgeRddBuilderMessage
case object RequestGraph extends EdgeRddBuilderMessage

class EdgeRddBuilder(sc: SparkContext) extends Actor {
	private var edges: EdgeRDD[Int] = newEdgeRDD

	def newEdgeRDD() = EdgeRDD.fromEdges(sc.parallelize(Array[Edge[Int]]()))

	def receive = {
		case Reset => edges = newEdgeRDD
		case AddEdge(edge) => {
			//TODO: Find some way to avoid this horrible type casting?
			val newRdd: EdgeRDD[Int] = EdgeRDD.fromEdges[Int, Int](sc.parallelize(List(edge.asInstanceOf[Edge[Int]])))
			edges = EdgeRDD.fromEdges(edges.union(newRdd))
		}
		case RequestGraph => {
			val verts = VertexRDD.fromEdges(edges, 1, 0)
			val graph = GraphImpl.fromExistingRDDs(verts, edges)
			sender ! SendGraph(graph)
		}
	}
}
