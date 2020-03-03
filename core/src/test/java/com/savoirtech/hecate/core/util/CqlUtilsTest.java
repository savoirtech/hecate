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

package com.savoirtech.hecate.core.util;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.savoirtech.hecate.test.TestUtils.assertUtilsClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.savoirtech.hecate.test.CassandraSingleton;
import java.time.Instant;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class CqlUtilsTest {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private List<ImmutablePair<DataType, Object>> columns;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createTable() throws Exception {
        columns = Arrays.asList(
                example(DataTypes.ASCII, "ascii_value"),
                example(DataTypes.BIGINT, 123456L),
                example(DataTypes.BOOLEAN, false),
                example(DataTypes.DOUBLE, 123.456),
                example(DataTypes.FLOAT, 123.456f),
                example(DataTypes.INT, 123456),
                example(DataTypes.DECIMAL, new BigDecimal("123.456")),
                example(DataTypes.INET, InetAddress.getLocalHost()),
                example(DataTypes.TEXT, "text_value"),
                example(DataTypes.TIMESTAMP, Instant.now()),
                example(DataTypes.TIMEUUID, UUID.fromString(new com.eaio.uuid.UUID().toString())),
                example(DataTypes.UUID, UUID.randomUUID()),
                example(DataTypes.VARINT, new BigInteger("1234567890")),
                example(DataTypes.BLOB, ByteBuffer.wrap("Hello, World!".getBytes())),
                example(new DefaultListType(DataTypes.TEXT, false), Lists.newArrayList("hello", "world")),
                example(new DefaultSetType(DataTypes.TEXT, false), Sets.newHashSet("hello", "world")),
                example(new DefaultMapType(DataTypes.TEXT, DataTypes.INT, false), Maps.asMap(Sets.newHashSet("one", "two", "three"), String::length)));

        CassandraSingleton.getSession().execute(SchemaBuilder.createTable("counter_table").ifNotExists().withPartitionKey("id", DataTypes.TEXT).withColumn("counter_value", DataTypes.COUNTER).build());
        CreateTable create = SchemaBuilder.createTable("test_table").ifNotExists().withPartitionKey("id", DataTypes.INT).withColumn("null_col", DataTypes.TEXT);
        RegularInsert insert = QueryBuilder.insertInto("test_table").value("id", literal(123)).value("null_col", literal(null));
        for (ImmutablePair<DataType, Object> column : columns) {
            DataType dataType = column.getLeft();
            String columnName = "test_" + dataType.getProtocolCode();
            create = create.withColumn(columnName, dataType);
            insert = insert.value(columnName, literal(column.getRight()));
        }

        CassandraSingleton.getSession().execute(create.build());
        CassandraSingleton.getSession().execute(insert.build());
    }

    private ImmutablePair<DataType, Object> example(DataType dataType, Object value) {
        return new ImmutablePair<>(dataType, value);
    }

    @Test
    public void testConstructor() {
        assertUtilsClass(CqlUtils.class);
    }

    @Test
    public void testGetNullValue() {
        List<Object> values = CqlUtils.toList(CassandraSingleton.getSession().execute(selectFrom("test_table").column("null_col").build()).one());
        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    @Test
    public void testBind() {
        PreparedStatement ps = CassandraSingleton.getSession().prepare(selectFrom("test_table").column("id").whereColumn("id").isEqualTo(bindMarker()).build());

        BoundStatement bound = CqlUtils.bind(ps, new Object[]{123});
        assertEquals(1, CassandraSingleton.getSession().execute(bound).all().size());
    }

    @Test
    public void testGetValue() {
        CassandraSingleton.withSession(session -> {
            Select selection = selectFrom("test_table").column("id");
            for (ImmutablePair<DataType, Object> column : columns) {
                selection = selection.column("test_" + column.getLeft().getProtocolCode());
            }
            Select select = selection.whereColumn("id").isEqualTo(literal(123));
            ResultSet result = session.execute(select.build());
            List<Object> objects = CqlUtils.toList(result.one());
            for (int i = 0; i < columns.size(); ++i) {
                ImmutablePair<DataType, Object> column = columns.get(i);
                assertEquals("Column test_" + column.getLeft().getProtocolCode() + " did not match.", column.getRight(), objects.get(i + 1));
            }
        });
    }

    @Test
    public void testGetValueInvalidType() {
        CassandraSingleton.withSession(session -> {
            session.execute("update counter_table set counter_value = counter_value + 1 where id = '1'");
            ResultSet rs = session.execute("select * from counter_table");
            List<Object> result = CqlUtils.toList(rs.one());
            assertTrue(result.contains("1"));
            assertTrue(result.contains(1L));
        });
    }
}