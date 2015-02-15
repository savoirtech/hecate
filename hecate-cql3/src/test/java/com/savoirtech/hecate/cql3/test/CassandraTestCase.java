/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraTestCase extends Assert {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    protected static final String KEYSPACE_NAME = "hecate";

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private Cluster cluster;

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    protected Cluster getCluster() {
        return cluster;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected Session connect() {
        return getCluster().connect(KEYSPACE_NAME);
    }

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
}
