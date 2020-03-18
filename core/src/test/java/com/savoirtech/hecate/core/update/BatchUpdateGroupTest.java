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

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.savoirtech.hecate.test.CassandraSingleton;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;

public class BatchUpdateGroupTest {

    @After
    public void after() {
        CassandraSingleton.clean();
    }

    @Test
    public void testAddUpdate() {
        BatchUpdateGroup group = new BatchUpdateGroup(CassandraSingleton.getSession(), Executors.newSingleThreadExecutor());
        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", literal("baz")).build());
        assertEquals(1, group.getBatchStatement().size());
    }

    @Test
    public void testComplete() {
        BatchUpdateGroup group = new BatchUpdateGroup(CassandraSingleton.getSession(), Executors.newSingleThreadExecutor());
        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", literal("baz")).build());

        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).build());
        group.complete();
        assertEquals(1, CassandraSingleton.getSession().execute("select bar from foo").all().size());
    }

    @Test
    public void testCompleteAsync() throws Exception {
        BatchUpdateGroup group = new BatchUpdateGroup(CassandraSingleton.getSession(), DefaultBatchType.UNLOGGED, Executors.newSingleThreadExecutor());
        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", literal("baz")).build());

        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).build());
        group.completeAsync().get();
        assertEquals(1, CassandraSingleton.getSession().execute("select bar from foo").all().size());
    }
}