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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.test.Cassandra;
import com.savoirtech.hecate.test.CassandraTestCase;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

@Cassandra
public class CqlUtilsTest extends CassandraTestCase {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private List<ImmutablePair<DataType, Object>> columns;

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Before
    public void createTable() throws Exception {
        TupleType tupleType = TupleType.of(DataType.text(), DataType.cint());
        columns = Arrays.asList(
                example(DataType.ascii(), "ascii_value"),
                example(DataType.bigint(), 123456L),
                example(DataType.cboolean(), false),
                example(DataType.cdouble(), 123.456),
                example(DataType.cfloat(), 123.456f),
                example(DataType.cint(), 123456),
                example(DataType.decimal(), new BigDecimal("123.456")),
                example(DataType.inet(), InetAddress.getLocalHost()),
                example(DataType.text(), "text_value"),
                example(DataType.timestamp(), new Date()),
                example(DataType.timeuuid(), UUID.fromString(new com.eaio.uuid.UUID().toString())),
                example(DataType.uuid(), UUID.randomUUID()),
                example(DataType.varchar(), "varchar_value"),
                example(DataType.varint(), new BigInteger("1234567890")),
                example(tupleType, tupleType.newValue("Hello", 1)),
                example(DataType.blob(), ByteBuffer.wrap("Hello, World!".getBytes())),
                example(DataType.list(DataType.text()), Lists.newArrayList("hello", "world")),
                example(DataType.set(DataType.text()), Sets.newHashSet("hello", "world")),
                example(DataType.map(DataType.text(), DataType.cint()), Maps.asMap(Sets.newHashSet("one", "two", "three"), String::length))
        );

        getSession().execute(SchemaBuilder.createTable("counter_table").addPartitionKey("id", DataType.varchar()).addColumn("counter_value", DataType.counter()));
        final Create create = SchemaBuilder.createTable("test_table").addPartitionKey("id", DataType.cint()).addColumn("null_col", DataType.varchar());
        Insert insert = QueryBuilder.insertInto("test_table").value("id", 123).value("null_col", null);
        for (ImmutablePair<DataType, Object> column : columns) {
            DataType dataType = column.getLeft();
            String columnName = "test_" + dataType.getName();
            create.addColumn(columnName, dataType);
            insert.value(columnName, column.getRight());
        }

        withSession(session -> {
            session.execute(create);
            session.execute(insert);
        });
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
        List<Object> values = CqlUtils.toList(getSession().execute(select("null_col").from("test_table")).one());
        assertEquals(1, values.size());
        assertNull(values.get(0));
    }

    @Test
    public void testBind() {
        Logger.getLogger(CqlUtils.class).setLevel(Level.DEBUG);
        PreparedStatement ps = getSession().prepare(select("id").from("test_table").where(eq("id", bindMarker())));

        BoundStatement bound = CqlUtils.bind(ps, new Object[]{123});
        assertEquals(1, getSession().execute(bound).all().size());
    }

    @Test
    public void testTupleValueToList() {
        TupleType tupleType = TupleType.of(
                DataType.varchar(),
                DataType.cboolean(),
                DataType.cint(),
                DataType.cfloat(),
                DataType.cdouble());
        TupleValue tupleValue = tupleType.newValue("foo", false, 123, 123.456f, 234.567);
        List<Object> values = CqlUtils.toList(tupleValue);
        assertEquals(Lists.newArrayList("foo", false, 123, 123.456f, 234.567), values);

    }

    @Test
    public void testGetValue() throws Exception {
        withSession(session -> {
            Select.Selection selection = select().column("id");
            columns.stream().map(col -> "test_" + col.getLeft().getName()).forEach(selection::column);
            Select.Where select = selection.from("test_table").where(eq("id", 123));
            ResultSet result = session.execute(select);
            List<Object> objects = CqlUtils.toList(result.one());
            for (int i = 0; i < columns.size(); ++i) {
                ImmutablePair<DataType, Object> column = columns.get(i);
                assertEquals("Column test_" + column.getLeft().getName() + " did not match.", column.getRight(), objects.get(i + 1));
            }
        });
    }

    @Test(expected = HecateException.class)
    public void testGetValueInvalidType() {
        withSession(session -> {
            session.execute("update counter_table set counter_value = counter_value + 1 where id = '1'");
            ResultSet rs = session.execute("select * from counter_table");
            CqlUtils.toList(rs.one());
        });
    }
}