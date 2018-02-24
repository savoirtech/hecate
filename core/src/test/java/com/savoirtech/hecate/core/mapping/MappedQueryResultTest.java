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

package com.savoirtech.hecate.core.mapping;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.savoirtech.hecate.test.Cassandra;
import com.savoirtech.hecate.test.CassandraTestCase;
import io.reactivex.Flowable;
import org.junit.Before;
import org.junit.Test;

@Cassandra
public class MappedQueryResultTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createTables() {
        withSession(session -> {
            session.execute("create table if not exists test (id int primary key, value varchar)");
            session.execute("insert into test (id,value) values (1, 'one')");
            session.execute("insert into test (id,value) values (2, 'two')");
            session.execute("insert into test (id,value) values (3, 'three')");
        });
    }

    @Test
    public void testIterator() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test"), row -> row.getString(1));
            Iterator<String> iterator = resultSet.iterator();
            assertEquals("one", iterator.next());
            assertEquals("two", iterator.next());
            assertEquals("three", iterator.next());
            assertFalse(iterator.hasNext());
        });
    }

    @Test
    public void testIterableOfRows() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test").all(), row -> row.getString(1));
            Iterator<String> iterator = resultSet.iterator();
            assertEquals("one", iterator.next());
            assertEquals("two", iterator.next());
            assertEquals("three", iterator.next());
            assertFalse(iterator.hasNext());
        });
    }

    @Test
    public void testList() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test"), row -> row.getString(1));
            List<String> results = resultSet.list();
            assertTrue(results.contains("one"));
            assertTrue(results.contains("two"));
            assertTrue(results.contains("three"));
        });
    }

    @Test
    public void testFlowable() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test"), row -> row.getString(1));
            Flowable<String> results = resultSet.flowable();
            List<String> list = results.toList().blockingGet();
            assertTrue(list.contains("one"));
            assertTrue(list.contains("two"));
            assertTrue(list.contains("three"));
        });
    }

    @Test
    public void testOne() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test"), row -> row.getString(1));
            assertEquals("one", resultSet.one());
        });
    }

    @Test
    public void testOneWithNoResults() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test where id = 5"), row -> row.getString(1));
            assertNull(resultSet.one());
        });
    }

    @Test
    public void testStream() {
        withSession(session -> {
            MappedQueryResult<String> resultSet = new MappedQueryResult<>(session.execute("select id, value from test"), row -> row.getString(1));
            Set<String> strings = resultSet.stream().map(String::toUpperCase).collect(Collectors.toSet());
            assertEquals(3, strings.size());
            assertTrue(strings.contains("ONE"));
            assertTrue(strings.contains("TWO"));
            assertTrue(strings.contains("THREE"));
        });
    }
}