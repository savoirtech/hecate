/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.core.test;

import com.google.common.collect.Maps;
import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import me.prettyprint.cassandra.locking.HLockManagerImpl;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.FailoverPolicy;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.locking.HLockManagerConfigurator;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final String CLUSTER_ADDRESS = "localhost:9171";

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected Cluster cluster;
    protected CassandraKeyspaceConfigurator keyspaceConfigurator;
    protected HLockManagerImpl hLockManager;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------


    protected ConsistencyLevelPolicy consistencyLevelPolicy() {
        return new AllOneConsistencyLevelPolicy();
    }

    protected FailoverPolicy failoverPolicy() {
        return FailoverPolicy.ON_FAIL_TRY_ALL_AVAILABLE;
    }

    protected CassandraHostConfigurator hostConfigurator() {
        CassandraHostConfigurator conf = new CassandraHostConfigurator();
        conf.setHosts(CLUSTER_ADDRESS);
        return conf;
    }

    protected String getClusterName() {
        return "HecateCluster";
    }

    protected String getKeyspaceName() {
        return getClass().getSimpleName();
    }

    @Before
    public void initializeCassandra() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
        cluster = HFactory.getOrCreateCluster(getClusterName(), CLUSTER_ADDRESS);

        HFactory.createKeyspace(getKeyspaceName(), cluster);
        keyspaceConfigurator = new CassandraKeyspaceConfigurator(hostConfigurator(), getKeyspaceName(), failoverPolicy(), consistencyLevelPolicy(), Maps.<String, String>newTreeMap());
        HLockManagerConfigurator hLockManagerConfigurator = new HLockManagerConfigurator();
        hLockManagerConfigurator.setReplicationFactor(1);

        hLockManager = new HLockManagerImpl(cluster, hLockManagerConfigurator);
        hLockManager.init();
    }
}