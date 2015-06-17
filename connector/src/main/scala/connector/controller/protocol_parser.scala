package connector.controller

import org.apache.spark.graphx.Edge

class PyScMessageException(msg: String) extends RuntimeException(msg)

/** Factory for [[connector.controller.PyScMessage] instances. */
object PyScMessage {
	def parse(str: String): PyScMessage = {
		// If the message starts with a 'c', it's a control message.
		if(str(0) == 'c') {
			// If it's a command message, parse based on it's int value.
			str.drop(1).toInt match {
				case 1 => ListenForEdges
				case 2 => FinishedMapping
				case 3 => CCsWritten
				case 4 => Shutdown
				case _ => throw new PyScMessageException("Unrecognized control message \"" + str + "\"")
			}
		} 
		// Otherwise, it's a data message.
		else {
			var edges: List[Edge[Int]] = List()
			for(pair <- str.split(";")) {
				val ids = pair.split(" ")
				edges = edges :+ Edge(ids(0).toLong, ids(1).toLong, 0) //TODO: Make this return edge type Nothing
			}
			DataMessage(edges)
		}
	}
}

/** Serializes and stores messages in the Python <-> Scala
 *  protocol. Inheriting classes represent the actual messages.
 */
sealed abstract class PyScMessage
sealed abstract class ControlMessage extends PyScMessage {
	val serialN: Int
	
	override def toString = s"c$serialN"
}
case object ListenForEdges extends ControlMessage { val serialN = 1 }
case object FinishedMapping extends ControlMessage { val serialN = 2 }
case object CCsWritten extends ControlMessage { val serialN = 3 }
case object Shutdown extends ControlMessage {val serialN = 4 }
case class DataMessage(val edges: List[Edge[Int]]) extends PyScMessage {
	override def toString = {
		var str = ""
		for(i <- 0 until edges.length) {
			str += edges(i).srcId
			str += " "
			str += edges(i).dstId
		    if(i < edges.length - 1)
		    	str += ";"
		}
		str
	}
}