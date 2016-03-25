/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

import java.lang.reflect.Method;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRule implements MethodRule {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private Session session;
    private Method currentMethod;

//----------------------------------------------------------------------------------------------------------------------
// MethodRule Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        Cassandra cassandra = method.getAnnotation(Cassandra.class);
        if (cassandra == null) {
            cassandra = method.getMethod().getDeclaringClass().getAnnotation(Cassandra.class);
        }
        if (cassandra == null) {
            currentMethod = method.getMethod();
            return new TimedStatement(base, LoggerFactory.getLogger(target.getClass()), method);
        }
        return new CassandraStatement(cassandra, base, LoggerFactory.getLogger(target.getClass()), method);
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Session getSession() {
        if(session == null) {
            throw new IllegalStateException("Method " + currentMethod.getName() + "() not @Cassandra-annotated, Cassandra Session not available.");
        }
        return session;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class CassandraStatement extends Statement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final Cassandra cassandra;
        private final Statement inner;
        private final Logger logger;
        private final FrameworkMethod method;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public CassandraStatement(Cassandra cassandra, Statement inner, Logger logger, FrameworkMethod method) {
            this.cassandra = cassandra;
            this.inner = inner;
            this.logger = logger;
            this.method = method;
        }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public void evaluate() throws Throwable {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(cassandra.timeout());
            EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
            Cluster cluster = Cluster.builder().addContactPoint("localhost").withPort(cassandra.port()).build();
            try(Session tempSession = cluster.newSession()) {
                logger.debug("Creating keyspace {}...", cassandra.keyspace());
                tempSession.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", cassandra.keyspace()));
                logger.debug("Keyspace {} created successfully.", cassandra.keyspace());
            }
            session = cluster.connect(cassandra.keyspace());
            try {
                final long before = System.currentTimeMillis();
                inner.evaluate();
                logger.debug("{}(): {} ms", method.getName(), System.currentTimeMillis() - before);
            }
            finally {
                logger.debug("Closing session...");
                session.close();
                logger.debug("Closing cluster...");
                cluster.close();
                logger.debug("Cassandra shut down complete!");
            }
        }
    }

    private class TimedStatement extends Statement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

        private final Statement inner;
        private final Logger logger;
        private final FrameworkMethod method;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

        public TimedStatement(Statement inner, Logger logger, FrameworkMethod method) {
            this.inner = inner;
            this.logger = logger;
            this.method = method;
        }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

        @Override
        public void evaluate() throws Throwable {
            final long before = System.currentTimeMillis();
            inner.evaluate();
            logger.debug("{}(): {} ms", method.getName(), System.currentTimeMillis() - before);
        }
    }
}
