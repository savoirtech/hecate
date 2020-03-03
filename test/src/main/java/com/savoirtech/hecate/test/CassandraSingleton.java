/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.savoirtech.hecate.test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CassandraContainer;

public class CassandraSingleton {
    private static final Logger logger = LoggerFactory.getLogger(CassandraSingleton.class);
    private static final CassandraContainer container;

    public static final String KEYSPACE = "hecate";

    private static final CqlSession session;

    static {
        container = new CassandraContainer();
        container.start();

        CqlSessionBuilder keyspaceBuilder = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(container.getContainerIpAddress(), container.getMappedPort(CassandraContainer.CQL_PORT))).withLocalDatacenter("datacenter1");
        try (CqlSession keyspaceSession = keyspaceBuilder.build()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Creating keyspace {}...", KEYSPACE);
            }
            keyspaceSession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", KEYSPACE));
            if (logger.isDebugEnabled()) {
                logger.debug("Keyspace {} created successfully.", KEYSPACE);
            }
        }
        CqlSessionBuilder builder = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(container.getContainerIpAddress(), container.getMappedPort(CassandraContainer.CQL_PORT))).withLocalDatacenter("datacenter1").withKeyspace(KEYSPACE);
        session = builder.build();
        logger.info("Cassandra container created with IP: {} and PORT: {}", container.getContainerIpAddress(), container.getMappedPort(CassandraContainer.CQL_PORT));
    }

    public static CqlSession getSession() {
        return session;
    }

    public static void withSession(Consumer<CqlSession> consumer) {
        consumer.accept(getSession());
    }

    public static CqlSession getSystemSession() {
        CqlSessionBuilder builder = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(container.getContainerIpAddress(), container.getMappedPort(CassandraContainer.CQL_PORT))).withLocalDatacenter("datacenter1");
        return builder.build();
    }

    public static void clean() {
        Set<CqlIdentifier> tables = session.getMetadata().getKeyspace(session.getKeyspace().get()).get().getTables().keySet();
        tables.forEach(table -> session.execute(SchemaBuilder.dropTable(table).build()));
    }
}
