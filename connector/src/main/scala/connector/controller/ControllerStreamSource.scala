package connector.controller

import java.io.{BufferedReader, PrintWriter, InputStreamReader}
import java.net.Socket

/**
 * For testing and complexity purposes, refactor extracting a BufferedReader & PrintWriter from
 * the Socket instance from the Controller class. The package object for [[connector.controller]]
 * will contain an implicit, socket2ControllerStreamSource to wrap a socket in an instance of this
 * trait.
 */
trait ControllerStreamSource {

	val inputStream: BufferedReader
	val outputStream: PrintWriter
	
	/**
	 * Doesn't need to be overriden implemented in all cases, but useful to close a socket connection.
	 * In addition, closes the output & input streams.
	 */
	def close: Unit = {
		inputStream.close
		outputStream.close
	}
	
}

class SocketStreamSource(sock: Socket) extends ControllerStreamSource {
	val inputStream = new BufferedReader(new InputStreamReader(sock.getInputStream))
	val outputStream = new PrintWriter(sock.getOutputStream)
	
	override def close = {
		super.close
		sock.close
	}
}