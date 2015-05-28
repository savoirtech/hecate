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

package com.savoirtech.hecate.core.statement;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Before;
import org.junit.Test;

public class StatementOptionsBuilderTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private Statement statement;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createStatement() {
        statement = getSession().prepare("SELECT now() FROM system.local").bind();
    }

    @Test
    public void testConsistencyLevel() throws Exception {
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        assertEquals(ConsistencyLevel.ALL, statement.getConsistencyLevel());
        StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.EACH_QUORUM).build().applyTo(statement);
        assertEquals(ConsistencyLevel.EACH_QUORUM, statement.getConsistencyLevel());
    }

    @Test
    public void testDefaultTimestamp() throws Exception {
        statement.setDefaultTimestamp(999L);
        assertEquals(999L, statement.getDefaultTimestamp());
        StatementOptionsBuilder.defaultTimestamp(12345L).build().applyTo(statement);
        assertEquals(12345L, statement.getDefaultTimestamp());
    }

    @Test
    public void testDisableTracing() throws Exception {
        statement.enableTracing();
        assertTrue(statement.isTracing());
        StatementOptionsBuilder.disableTracing().build().applyTo(statement);
        assertFalse(statement.isTracing());
    }

    @Test
    public void testEnableTracing() throws Exception {
        statement.disableTracing();
        assertFalse(statement.isTracing());
        StatementOptionsBuilder.enableTracing().build().applyTo(statement);
        assertTrue(statement.isTracing());
    }

    @Test
    public void testFetchSize() throws Exception {
        statement.setFetchSize(123);
        assertEquals(123, statement.getFetchSize());
        StatementOptionsBuilder.fetchSize(456).build().applyTo(statement);
        assertEquals(456, statement.getFetchSize());
    }

    @Test
    public void testRetryPolicy() throws Exception {
        statement.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        assertEquals(FallthroughRetryPolicy.INSTANCE, statement.getRetryPolicy());
        StatementOptionsBuilder.retryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE).build().applyTo(statement);
        assertEquals(DowngradingConsistencyRetryPolicy.INSTANCE, statement.getRetryPolicy());
    }

    @Test
    public void testSerialConsistencyLevel() throws Exception {
        statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        assertEquals(ConsistencyLevel.SERIAL, statement.getSerialConsistencyLevel());
        StatementOptionsBuilder.serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL).build().applyTo(statement);
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, statement.getSerialConsistencyLevel());
    }

    @Test
    public void testEmpty() {
        statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement.setDefaultTimestamp(999L);
        statement.enableTracing();
        statement.setFetchSize(123);
        statement.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);

        StatementOptionsBuilder.empty().applyTo(statement);
        assertEquals(ConsistencyLevel.ALL, statement.getConsistencyLevel());
        assertEquals(999L, statement.getDefaultTimestamp());
        assertTrue(statement.isTracing());
        assertEquals(123, statement.getFetchSize());
        assertEquals(FallthroughRetryPolicy.INSTANCE, statement.getRetryPolicy());
        assertEquals(ConsistencyLevel.SERIAL, statement.getSerialConsistencyLevel());
    }
}