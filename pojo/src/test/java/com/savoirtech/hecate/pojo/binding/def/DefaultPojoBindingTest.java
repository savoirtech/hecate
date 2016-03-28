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

package com.savoirtech.hecate.pojo.binding.def;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.Cassandra;
import org.junit.Test;

public class DefaultPojoBindingTest extends AbstractDaoTestCase {

    @Test
    @Cassandra(keyspace = "bar")
    public void testValidateSchemaWithMissingTable() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        assertHecateException("Table \"foo\" not found in keyspace \"bar\".", () -> binding.verifySchema(getSession(), "foo"));
    }


    @Test
    @Cassandra
    public void testValidateSchemaWithMissingColumn() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("baz", DataType.varchar()));
        assertHecateException("Table \"foo\" does not contain column \"id\" of type \"varchar\".", () -> binding.verifySchema(getSession(), "foo"));
    }

    @Test
    @Cassandra
    public void testValidateSchemaWithColumnTypeMismatch() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("id", DataType.cint()));
        assertHecateException("Column \"id\" in table \"foo\" is of the wrong type \"int\" (expected \"varchar\").", () -> binding.verifySchema(getSession(), "foo"));
    }

    @Test
    @Cassandra
    public void testValidateSchemaWithNonPartitionKeySimple() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("baz", DataType.cint()).addColumn("id", DataType.varchar()));
        assertHecateException("Column \"id\" in table \"foo\" is not a partition key.", () -> binding.verifySchema(getSession(), "foo"));
    }

    @Test
    @Cassandra
    public void testValidateSchemaWithNonPartitionKeyComposite() {
        PojoBinding<CompositeKeyEntity> binding = getBindingFactory().createPojoBinding(CompositeKeyEntity.class);
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("baz", DataType.cint()).addColumn("id", DataType.varchar()));
        assertHecateException("Column \"id\" in table \"foo\" is not a partition key.", () -> binding.verifySchema(getSession(), "foo"));
    }

    @Test
    @Cassandra
    public void testValidateSchemaWithNonClusteringColumnComposite() {
        PojoBinding<CompositeKeyEntity> binding = getBindingFactory().createPojoBinding(CompositeKeyEntity.class);
        getSession().execute(SchemaBuilder.createTable("foo").addPartitionKey("id", DataType.varchar()).addColumn("cluster", DataType.varchar()));
        assertHecateException("Column \"cluster\" in table \"foo\" is not a clustering column.", () -> binding.verifySchema(getSession(), "foo"));
    }

    @Test
    public void testEquals() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        PojoBinding<SimpleKeyEntity> other = new DefaultPojoBindingFactory(getFacetProvider(), getConverterRegistry(), getNamingStrategy()).createPojoBinding(SimpleKeyEntity.class);

        assertEquals(binding, binding);
        assertEquals(binding, other);
        assertNotEquals(binding, null);
        assertNotEquals(binding, "Hello!");
    }

    public static class SimpleKeyEntity extends UuidEntity {

    }

    public static class CompositeKeyEntity extends UuidEntity {
        @ClusteringColumn
        private String cluster;
    }

}