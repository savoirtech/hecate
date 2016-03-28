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

package com.savoirtech.hecate.core.update;

import java.util.concurrent.Executors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.test.Cassandra;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.junit.Test;

@Cassandra
public class AsyncUpdateGroupTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Test
    public void testComplete() {
        AsyncUpdateGroup group = new AsyncUpdateGroup(getSession(), Executors.newSingleThreadExecutor());
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("bar", DataType.varchar()));

        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", "baz"));
        group.complete();

        assertEquals(1, getSession().execute("select bar from foo where bar = ?", "baz").all().size());
    }

    @Test
    public void testCompleteAsync() throws Exception {
        AsyncUpdateGroup group = new AsyncUpdateGroup(getSession(), Executors.newSingleThreadExecutor());
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("bar", DataType.varchar()));

        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", "baz"));
        group.completeAsync().get();

        assertEquals(1, getSession().execute("select bar from foo where bar = ?", "baz").all().size());
    }

    @Test(expected = HecateException.class)
    public void testWithInvalidQuery() throws Exception {
        AsyncUpdateGroup group = new AsyncUpdateGroup(getSession(), Executors.newSingleThreadExecutor());
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("bar", DataType.varchar()));

        group.addUpdate(QueryBuilder.insertInto("foo").value("bogus", "baz"));
        group.complete();
    }
}