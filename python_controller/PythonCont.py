__author__ = 'Matt'

MESSAGES = {'startEdge':'c1',
            'stopEdge':'c2',
            'cmpsWritten':'c3',
            'finished':'c4'}

import socket
import sys
import os
import random

lib_path = os.path.abspath(os.path.join('..', 'int_code_test'))
sys.path.append(lib_path)
lib_path = os.path.abspath(os.path.join('..', 'cass_controller'))
sys.path.append(lib_path)

import IntCass
import IntCode
import Cass


def send_edges(sock,vertex):
    '''Sends the edges of vertex through the socket sock'''

    nbsgen = vertex.neighborsgen()

    edges = []

    thisvert = ''.join(str(x) for x in vertex.ints)

    for i in nbsgen:
        edges.append(thisvert + '_' + ''.join(str(x) for x in i))

    message = ';'.join(edges)  + '#'

    print message

    try:

        # Send data
        sock.sendall(message)

    finally:
        print >>sys.stderr, 'closing socket'
        sock.close()

def get_vertices(n):
    '''Returns list of n random uid's of unmapped vertices'''

    numverts = 0

    DBcon = Cass.CassController(['10.104.251.45'])

    verts = []

    while numverts < n:

        numints = 0
        uid = []

        while numints < 5:
            uid.append(random.randint(0,9))
            numints += 1

        thisvert = DBcon.query("SELECT * FROM connector_{!s}.cmptable WHERE uid='{!s}'".format(str(uid[0]),''.join(str(uid[x]) for x in range(0,5))))

        for i in thisvert:
            if not i.mapped:
                verts.append(uid)
                numverts += 1

    return verts

#CLIENT CODE

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect the socket to the port where the server is listening
#server_address = ('localhost', 9998)
#print >>sys.stderr, 'connecting to %s port %s' % server_address
#sock.connect(server_address)

test = IntCode.IntCode([1,2,3,4,5])

print get_vertices(5)

#send_edges(sock,test)

#test = IntCode.IntCode([1,2,3,4])
#send_edges(sock,test)

