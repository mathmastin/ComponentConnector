__author__ = 'Matt Mastin'

import os, sys

lib_path = os.path.abspath(os.path.join('..', 'int_code_test'))
sys.path.append(lib_path)

import IntCode
import Cass


class IntKeyspace(Cass.CassController):
    """Subcalss of CassController for use with IntCodes stored on a Cassandra cluster

    Used for interacting with a NumStringPile that lives in a Cassandra keyspace
    """

    def __init__(self, stringsize = 1, hosts = None, keyspacePrefix = None):

        self.stringsize = stringsize

        if not keyspacePrefix:
            self.keyspacePrefix = 'connector'

        self.keyspace = self.keyspacePrefix + '_' + str(self.stringsize)

        super(IntKeyspace, self).__init__(hosts, self.keyspace)

    def deleteintkeyspace(self):
        """Deletes the keyspace associated to the NumKeyspace"""
        self.deletekeyspace()

    def intquery(self, cqlstatement):
        """Yields a generator into the results of the query cqlstatement
        Note that we shouldn't need to explicitly include the name of the
        keyspace
        """

        # We assume that if a query is called then the keyspace exists and should be used
        # if the keyspace doesn't exist then Cassandra will throw an InvalidRequest error
        # if the generator is used.
        self.usekeyspace(self.keyspace)

        results = self.query(cqlstatement)
        for i in results:
            # The query yields a unicode version of the digits, so we must format and cast
            # to a list of ints to send to the constructor of NumString

            pass # this needs to cast the elements in results to IntCodes and
                 # return a generator into that list

    def getnhbs(self, numstring):
        """Returns generator into list of neighbors of numstring in the NumString graph

        Two NumStrings are neighbors if one can be obtained from the other
        by adding 1 to some integer in the string and subtracting 1 from another
        """

        pass # This needs to use Jason's nhbr code to query the DB and return generator into the nhbs

    def dumpsubgraph(self, vertlist = None):
        """Dumps neighbor subgraph corresponding to the vertices in vertlist
        to file for processing by Graphx"""
        pass

    def updatecomps(self):
        """Reads connected component data from Graphx generated file
        and update component information in the NumString keyspace"""
        pass