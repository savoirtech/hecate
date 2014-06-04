package com.savoirtech.hecate.cql3.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected static final String KEYSPACE_NAME = "hecate";

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private Cluster cluster;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void initializeCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        cluster = Cluster.builder().addContactPoint("localhost").withPort(9142).build();
        Session session = getCluster().newSession();
        logger.debug("Creating keyspace {}...", KEYSPACE_NAME);
        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", KEYSPACE_NAME));
        logger.debug("Keyspace {} created successfully.", KEYSPACE_NAME);
        session.close();
    }

    protected Session connect() {
        return getCluster().connect(KEYSPACE_NAME);
    }

    protected Cluster getCluster() {
        return cluster;
    }
}
