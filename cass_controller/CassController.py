__author__ = 'Matt'

from cassandra.cluster import Cluster


class CassController(object):
    """Controller for interacting with a cassandra database."""

    def __init__(self, hosts=None, keyspace=None):
        """Connects to the cluster at hosts and accesses keyspace if provided"""
        if not hosts:
            hosts = ['localhost']
        self.cluster = Cluster(hosts)
        self.keyspace = keyspace

        self.session = self.cluster.connect(self.keyspace)

    def deletekeyspace(self):
        """Deletes the current keyspace if it is set."""
        if self.keyspace is not None:
            self.session.execute_async("DROP KEYSPACE IF EXISTS " + self.keyspace)
        else:
            raise ValueError("keyspace of None recieved by deletekeyspace")

    def query(self, cqlstatement):
        """Passes along a CQL query statement and returns result.
        Note that results may be paged, but return type behaves as an
        iterator the entire set of results.
        """
        return self.session.execute(cqlstatement)

    def usekeyspace(self, keyspace):
        """Sets the current session to use the given keyspace."""
        self.keyspace = keyspace

        if self.keyspace is not None:
            self.session.execute("USE " + self.keyspace)
