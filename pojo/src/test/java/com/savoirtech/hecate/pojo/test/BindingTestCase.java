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

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.Lists;
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
        Create create = SchemaBuilder.createTable(tableName);
        binding.describe(create, Lists.newLinkedList());
        assertEquals(cql, StringUtils.normalizeSpace(create.getQueryString()));
    }

    protected void assertDeleteEquals(KeyBinding binding, String cql) {
        assertDeleteEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertDeleteEquals(KeyBinding binding, String tableName, String cql) {
        Delete.Where delete = QueryBuilder.delete().from(tableName).where();
        binding.delete(delete);
        assertEquals(cql, delete.getQueryString());
    }

    protected void assertInsertEquals(ColumnBinding binding, String cql) {
        assertInsertEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertInsertEquals(ColumnBinding binding, String tableName, String cql) {
        Insert insert = QueryBuilder.insertInto(tableName);
        binding.insert(insert);
        assertEquals(cql, insert.getQueryString());
    }

    protected void assertSelectEquals(ColumnBinding binding, String cql)
    {
        assertSelectEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertSelectEquals(ColumnBinding binding, String tableName, String cql) {
        Select.Selection select = QueryBuilder.select();
        binding.select(select);
        assertEquals(cql, select.from(tableName).getQueryString());
    }

    protected void assertSelectWhereEquals(KeyBinding binding, String cql)
    {
        assertSelectWhereEquals(binding, DEFAULT_TABLE_NAME, cql);
    }

    protected void assertSelectWhereEquals(KeyBinding binding, String tableName, String cql) {
        Select.Selection select = QueryBuilder.select();
        binding.select(select);
        Select.Where where = select.from(tableName).where();
        binding.selectWhere(where);
        assertEquals(cql, where.getQueryString());
    }
}
