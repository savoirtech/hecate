package com.savoirtech.hecate.core;

import java.util.HashMap;
import java.util.Map;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import org.apache.cassandra.auth.IAuthenticator;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractCassandraTest {

    public static final String CLUSTER = "Cluster";
    public static final String KEYSPACE = "savoirtech";
    public static final String HOST = "localhost:9175";
    //Column Family
    public ConsistencyLevelPolicy consistencyLevelPolicy = new AllOneConsistencyLevelPolicy();
    public FailoverPolicy failoverPolicy = FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    public Map<String, String> credentials = new HashMap<String, String>();
    public CassandraKeyspaceConfigurator keyspaceConfigurator;

    protected AbstractCassandraTest() {
        credentials.put(IAuthenticator.USERNAME_KEY, "admin");
        credentials.put(IAuthenticator.PASSWORD_KEY, "secret");
    }

    @Before
    public void setUp() throws Exception {

        credentials.put(IAuthenticator.USERNAME_KEY, "admin");
        credentials.put(IAuthenticator.PASSWORD_KEY, "secret");
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", getCredentials());
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        DataLoader dataLoader = new DataLoader(CLUSTER, HOST, credentials);
        dataLoader.load(new ClassPathYamlDataSet("dataset.yaml"));

        keyspaceConfigurator = new CassandraKeyspaceConfigurator(getHost(), KEYSPACE, getFailoverPolicy(), getConsistencyLevelPolicy(),
            getCredentials());
    }

    @After
    public void after() throws Exception {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    protected CassandraHostConfigurator getHost() {

        CassandraHostConfigurator conf = new CassandraHostConfigurator();
        conf.setHosts(HOST);
        return conf;
    }

    protected ConsistencyLevelPolicy getConsistencyLevelPolicy() {
        return consistencyLevelPolicy;
    }

    protected FailoverPolicy getFailoverPolicy() {
        return failoverPolicy;
    }

    public Map<String, String> getCredentials() {
        return credentials;
    }
}
