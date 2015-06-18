package connector

import java.net.Socket

package object controller {
	
	implicit def socket2ControllerStreamSource(sock: Socket) = new SocketStreamSource(sock)
	
}