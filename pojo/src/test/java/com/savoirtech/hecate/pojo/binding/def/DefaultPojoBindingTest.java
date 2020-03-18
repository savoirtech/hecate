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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.entities.UuidEntity;
import com.savoirtech.hecate.pojo.test.AbstractDaoTestCase;
import com.savoirtech.hecate.test.CassandraSingleton;
import org.junit.After;
import org.junit.Test;

public class DefaultPojoBindingTest extends AbstractDaoTestCase {

    @After
    public void after() {
        CassandraSingleton.clean();
    }

    @Test
    public void testValidateSchemaWithMissingTable() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        assertHecateException("Table \"doesnotexist\" not found in keyspace \"hecate\".", verifySchema(binding, "doesnotexist"));
    }

    private Runnable verifySchema(PojoBinding<?> binding, String tableName) {
        return () -> binding.verifySchema(CassandraSingleton.getSession().getMetadata().getKeyspace(CassandraSingleton.getSession().getKeyspace().get()).get(), tableName);
    }


    @Test
    public void testValidateSchemaWithMissingColumn() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("baz", DataTypes.TEXT).build());
        assertHecateException("Table \"foo\" does not contain column \"id\" of type \"text\".", verifySchema(binding, "foo"));
    }

    @Test
    public void testValidateSchemaWithColumnTypeMismatch() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("id", DataTypes.INT).build());
        assertHecateException("Column \"id\" in table \"foo\" is of the wrong type \"int\" (expected \"text\").", verifySchema(binding, "foo"));
    }

    @Test
    public void testValidateSchemaWithNonPartitionKeySimple() {
        PojoBinding<SimpleKeyEntity> binding = getBindingFactory().createPojoBinding(SimpleKeyEntity.class);
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("baz", DataTypes.INT).withColumn("id", DataTypes.TEXT).build());
        assertHecateException("Column \"id\" in table \"foo\" is not a partition key.", verifySchema(binding, "foo"));
    }

    @Test
    public void testValidateSchemaWithNonPartitionKeyComposite() {
        PojoBinding<CompositeKeyEntity> binding = getBindingFactory().createPojoBinding(CompositeKeyEntity.class);
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("baz", DataTypes.INT).withColumn("id", DataTypes.TEXT).build());
        assertHecateException("Column \"id\" in table \"foo\" is not a partition key.", verifySchema(binding, "foo"));
    }

    @Test
    public void testValidateSchemaWithNonClusteringColumnComposite() {
        PojoBinding<CompositeKeyEntity> binding = getBindingFactory().createPojoBinding(CompositeKeyEntity.class);
        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("foo").withPartitionKey("id", DataTypes.TEXT).withColumn("cluster", DataTypes.TEXT).build());
        assertHecateException("Column \"cluster\" in table \"foo\" is not a clustering column.", verifySchema(binding, "foo"));
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