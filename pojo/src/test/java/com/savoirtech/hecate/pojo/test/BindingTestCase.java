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

package com.savoirtech.hecate.pojo.test;

import static org.junit.Assert.assertEquals;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.savoirtech.hecate.core.schema.Schema;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import org.apache.commons.lang3.StringUtils;

public class BindingTestCase extends AbstractDaoTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String DEFAULT_TABLE_NAME = "foo";

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void assertCreateEquals(ColumnBinding binding, String cql) {
        assertCreateEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertCreateEquals(ColumnBinding binding, String tableName, String cql) {
        Schema schema = new Schema();
        binding.describe(schema.createTable(tableName), schema);
        SimpleStatement statement = schema.items().findFirst().get().createStatement();
        assertEquals(cql, StringUtils.normalizeSpace(statement.getQuery()));
    }

    protected void assertDeleteEquals(KeyBinding binding, String cql) {
        assertDeleteEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertDeleteEquals(KeyBinding binding, String tableName, String cql) {
        Delete delete = QueryBuilder.deleteFrom(tableName).where();
        delete = binding.delete(delete);
        assertEquals(cql, delete.toString());
    }

    protected void assertInsertEquals(ColumnBinding binding, String cql) {
        assertInsertEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertInsertEquals(ColumnBinding binding, String tableName, String cql) {
        InsertInto insertInto = QueryBuilder.insertInto(tableName);
        Insert insert = binding.insert(insertInto);
        assertEquals(cql, insert.toString());
    }

    protected void assertSelectEquals(ColumnBinding binding, String cql)
    {
        assertSelectEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertSelectEquals(ColumnBinding binding, String tableName, String cql) {
        SelectFrom selectFrom = QueryBuilder.selectFrom(tableName);
        Select select = binding.select(selectFrom);
        assertEquals(cql, select.toString());
    }

    protected void assertSelectWhereEquals(KeyBinding binding, String cql)
    {
        assertSelectWhereEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertSelectWhereEquals(KeyBinding binding, String tableName, String cql) {
        SelectFrom selectFrom = QueryBuilder.selectFrom(tableName);
        Select select = binding.select(selectFrom);
        select = binding.selectWhere(select);
        assertEquals(cql, select.toString());
    }

    public static void main(String[] args) {
        SimpleStatement create = SchemaBuilder.createTable("foo").withPartitionKey("bar", DataTypes.TEXT).withClusteringColumn("baz", DataTypes.TEXT).withClusteringOrder("baz", ClusteringOrder.DESC).build();
        System.out.println(create.toString());
    }
}
