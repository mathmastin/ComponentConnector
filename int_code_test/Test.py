import sys
sys.path.insert(0, '../set_up')
from SetUp import *
from IntCode import *
from cassandra.cluster import Cluster


def generate_l5s():
    digits = range(10)
    for i1 in digits:
        for i2 in digits:
            for i3 in digits:
                for i4 in digits:
                    for i5 in digits:
                        code = IntCode([i1,i2,i3,i4,i5])
                        yield code.hash()

cluster = Cluster(['10.104.251.45'])
setUp = ConnectorSetUp(cluster, nKeyspaces = 10, identifyKspc = IntCode.uidToKspc)
setUp.connect()
setUp.setUpKeyspaces(opts = [('replication_factor', 1)])
setUp.populateTable(generate_l5s())
