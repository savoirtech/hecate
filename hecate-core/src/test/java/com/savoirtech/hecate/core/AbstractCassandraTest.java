/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core;

import java.util.HashMap;
import java.util.Map;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import me.prettyprint.cassandra.locking.HLockManagerImpl;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.locking.HLockManager;
import me.prettyprint.hector.api.locking.HLockManagerConfigurator;
import org.apache.cassandra.auth.IAuthenticator;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractCassandraTest {

    public static final String CLUSTER = "Cluster";
    public static final String KEYSPACE = "hecate";
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

        HLockManagerConfigurator hLockManagerConfigurator = new HLockManagerConfigurator();
        hLockManagerConfigurator.setReplicationFactor(1);
        HLockManager hLockManager = new HLockManagerImpl(HFactory.getOrCreateCluster(CLUSTER, KEYSPACE), hLockManagerConfigurator);
        hLockManager.init();
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
