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

        #yield self.query(cqlstatement)
        results = self.query(cqlstatement)
        for i in results:
            yield IntCode.IntCode(map(int,i.uid))

    def getnhbs(self, intcode):
        """Returns generator into list of neighbors of IntCodes in the IntCode graph

        Two IntCodes are neighbors if one can be obtained from the other
        by adding 1 to some integer in the string and subtracting 1 from another
        """
        nhbsgen = intcode.neighborsgen()

        for i in nhbsgen:
            yield self.intquery("SELECT * FROM cmptable WHERE uid "
                                "= '{!s}'".format(''.join(str(i[x]) for x in range(0,len(i)))))
