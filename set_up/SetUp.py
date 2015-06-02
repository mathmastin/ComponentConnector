class ConnectorSetUp:
    """
    The class responsible for setting up the tables in the Cassandra database
    that will be used in the map & reduce steps later on.

    The keyspaces created will be named by the form 'PREFIX-N', where PREFIX
    is specified in the constructor and N is an integer incrementing from 0.

    The basic useage of the class is:
    1. Initialize an instance of the class.
    2. Connect to the Cassandra database.
    3. Generate the keyspaces & tables.
    4. Populate the tables.
    """

    def __init__(self, cluster, nKeyspaces, identifyKspc, keyspacePrefix = 'Connector'):
        """
        Store the cluster object, but don't connect to it yet.

        Args:
            cluster (DATASTAX cluster): The cluster object to target.
                By allowing the user to initialize the cluster object, they
                retain full control over the IP, port, load policy, etc.
            nKeyspaces (integer): The number of keyspaces in the domain.
            identifyKspc ( (uid) -> int ): A function that takes a uid and returns the
                corresponding keyspace.
            keyspacePrefix (string, optional): Prefix used for naming the keyspaces.
                Defaults to 'Connector'.
        """
        self.cluster = cluster
        self.keyspaceNames = [keyspacePrefix + '-' + `i` for i in range(nKeyspaces)]
        self.identifyKspc = identifyKspc

    def connect(self):
        """
        Call connect on the cluster object.
        """
        self.session = self.cluster.connect()

    #TODO: Isolate all specific database bits to helper functions?
    #TODO: Use asynchronous queries
    def setUpKeyspaces(self, strategyClass = 'SimpleStrategy', *args):
        """
        Create the necessary keyspaces & tables with the given strategy options.
        All keyspaces use the same strategies. See the CQL documentation for
        CREATE KEYSPACE for more details.

        Args:
            strategyClass (string, optional): Defaults to 'SimpleStrategy'.
            *args: Variable length list of tuples (strategy name (string), strategy val (string)).
        """
        for kspcName in self.keyspaceNames:
            # Create the keyspace
            query = "CREATE KEYSPACE %(kspcName)s WITH strategy_class = \'%(strategyClass)s\'" % locals()
            for opt in args:
                optName = opt[0]
                optVal = opt[1]
                query += " AND strategy_options:%(optName)s = %(optVal)s" % locals()
            query += ';'
            self.session.execute(query)
            # Create the table
            #TODO: Uid type?
            query = """CREATE TABLE %(kspcName)s.cmpTable (
                        uid varchar,
                        cmp list<int>,
                        mapped boolean,
                        PRIMARY KEY (uid, cmp)
                    );"""
            #TODO: Use cluster ordering on this table?
            self.session.execute(query)

    #TODO: Use asynchronous queries
    def populateTable(self, uidGenerator):
        """
        Populate the cmpTable's with uid's, cmp numbers, and a false mapped value.

        Args:
            uidGenerator (generator): Stream the uid's to this function."""
        currentCmp = 0
        for uid in uidGenerator:
            kspcName = self.keyspaceNames[self.identifyKspc(uid)]
            query = "INSERT INTO %(kspcName)s.cmpTable (uid, cmp, mapped) VALUES (%(uid)s, %(currentCmp)s, false)" % locals()
            self.session.execute(query)
