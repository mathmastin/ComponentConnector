package connector.controller

import org.apache.spark.graphx.{VertexRDD, VertexId}

/**
 * For testing and complexity purposes, extract writing the CC's to the
 * Cassandra database to a trait.
 */
trait ComponentsWriter {

	def writeCCs(vertices: VertexRDD[VertexId])
	
}

/**
 * Write the connected component data to a Cassandra database.
 * TODO: Implement this.
 */
class CassandraWriter extends ComponentsWriter {
	
	def writeCCs(vertices: VertexRDD[VertexId]) {}
	
}