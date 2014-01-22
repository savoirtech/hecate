package com.savoirtech.hecate.test;

import me.prettyprint.cassandra.dao.SimpleCassandraDao;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public abstract class CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String CLUSTER_ADDRESS = "localhost:9171";
    protected Keyspace keyspace;
    protected Cluster cluster;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void createColumnFamily(String columnFamilyName) {
        createColumnFamily(columnFamilyName, ComparatorType.BYTESTYPE);
    }

    protected void createColumnFamily(String columnFamilyName, ComparatorType comparatorType) {
        ColumnFamilyDefinition definition = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), columnFamilyName, comparatorType);
        cluster.addColumnFamily(definition, true);
    }

    protected String getClusterName() {
        return "TestCluster";
    }

    protected String getColumnValueAsString(String columnFamilyName, String key, String columnName) {
        SimpleCassandraDao dao = new SimpleCassandraDao();
        dao.setColumnFamilyName(columnFamilyName);
        dao.setKeyspace(keyspace);
        return dao.get(key, columnName);
    }

    protected String getKeyspaceName() {
        return getClass().getSimpleName();
    }

    @Before
    public void initializeCassandra() throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        cluster = HFactory.getOrCreateCluster(getClusterName(), CLUSTER_ADDRESS);
        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(getKeyspaceName(), ThriftKsDef.DEF_STRATEGY_CLASS, 1, Collections.<ColumnFamilyDefinition>emptyList());
        cluster.addKeyspace(keyspaceDefinition, true);
        keyspace = HFactory.createKeyspace(getKeyspaceName(), cluster);
    }
}
