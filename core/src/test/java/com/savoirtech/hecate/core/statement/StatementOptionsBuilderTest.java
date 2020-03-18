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

import static com.savoirtech.hecate.test.CassandraSingleton.getSession;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.junit.Before;
import org.junit.Test;

public class StatementOptionsBuilderTest {
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
        statement = statement.setConsistencyLevel(ConsistencyLevel.ALL);
        assertEquals(ConsistencyLevel.ALL, statement.getConsistencyLevel());
        statement = StatementOptionsBuilder.consistencyLevel(ConsistencyLevel.EACH_QUORUM).build().applyTo(statement);
        assertEquals(ConsistencyLevel.EACH_QUORUM, statement.getConsistencyLevel());
    }

    @Test
    public void testDefaultTimestamp() throws Exception {
        statement = statement.setQueryTimestamp(999L);
        assertEquals(999L, statement.getQueryTimestamp());
        statement = StatementOptionsBuilder.defaultTimestamp(12345L).build().applyTo(statement);
        assertEquals(12345L, statement.getQueryTimestamp());
    }

    @Test
    public void testDisableTracing() throws Exception {
        statement = statement.setTracing(true);
        assertTrue(statement.isTracing());
        statement = StatementOptionsBuilder.disableTracing().build().applyTo(statement);
        assertFalse(statement.isTracing());
    }

    @Test
    public void testEnableTracing() throws Exception {
        statement = statement.setTracing(false);
        assertFalse(statement.isTracing());
        statement = StatementOptionsBuilder.enableTracing().build().applyTo(statement);
        assertTrue(statement.isTracing());
    }

    @Test
    public void testFetchSize() throws Exception {
        statement = statement.setPageSize(123);
        assertEquals(123, statement.getPageSize());
        statement = StatementOptionsBuilder.fetchSize(456).build().applyTo(statement);
        assertEquals(456, statement.getPageSize());
    }

    @Test
    public void testSerialConsistencyLevel() throws Exception {
        statement = statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        assertEquals(ConsistencyLevel.SERIAL, statement.getSerialConsistencyLevel());
        statement = StatementOptionsBuilder.serialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL).build().applyTo(statement);
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, statement.getSerialConsistencyLevel());
    }

    @Test
    public void testEmpty() {
        statement = statement.setConsistencyLevel(ConsistencyLevel.ALL);
        statement = statement.setQueryTimestamp(999L);
        statement = statement.setTracing(true);
        statement = statement.setPageSize(123);
        statement = statement.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);

        statement = StatementOptionsBuilder.empty().applyTo(statement);
        assertEquals(ConsistencyLevel.ALL, statement.getConsistencyLevel());
        assertEquals(999L, statement.getQueryTimestamp());
        assertTrue(statement.isTracing());
        assertEquals(123, statement.getPageSize());
        assertEquals(ConsistencyLevel.SERIAL, statement.getSerialConsistencyLevel());
    }
}