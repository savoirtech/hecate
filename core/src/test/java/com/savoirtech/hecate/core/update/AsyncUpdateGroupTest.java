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
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.test.CassandraSingleton;
import org.junit.After;
import org.junit.Test;

public class AsyncUpdateGroupTest {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @After
    public void after() {
        CassandraSingleton.clean();
    }

    @Test
    public void testComplete() {
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).build());

        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", literal("baz")).build());
        group.complete();

        assertEquals(1, CassandraSingleton.getSession().execute(selectFrom("foo").all().whereColumn("bar").isEqualTo(literal("baz")).build()).all().size());
    }

    @Test
    public void testCompleteAsync() throws Exception {
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).build());

        group.addUpdate(QueryBuilder.insertInto("foo").value("bar", literal("baz")).build());
        group.completeAsync().get();

        assertEquals(1, CassandraSingleton.getSession().execute(selectFrom("foo").all().whereColumn("bar").isEqualTo(literal("baz")).build()).all().size());
    }

    @Test(expected = HecateException.class)
    public void testWithInvalidQuery() throws Exception {
        AsyncUpdateGroup group = new AsyncUpdateGroup(CassandraSingleton.getSession());
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).build());

        group.addUpdate(QueryBuilder.insertInto("foo").value("bogus", literal("baz")).build());
        group.complete();
    }
}