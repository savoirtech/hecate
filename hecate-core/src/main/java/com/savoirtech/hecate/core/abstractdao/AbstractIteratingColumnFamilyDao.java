/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core.abstractdao;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import com.savoirtech.hecate.core.annotations.CompositeComponent;
import com.savoirtech.hecate.core.annotations.CompositeComponentProcessor;
import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.config.HectorManager;
import com.savoirtech.hecate.core.utils.ColumnIterator;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

public abstract class AbstractIteratingColumnFamilyDao<KeyType, NameType, ValueType> {

    /**
     * The key space.
     */
    protected final Keyspace keySpace;
    /**
     * The column family name.
     */
    protected final String columnFamilyName;
    /**
     * The name type class.
     */
    private final Class<NameType> nameClass;
    /**
     * The value type class.
     */
    private final Class<ValueType> valueClass;
    /**
     * The key type class.
     */
    private final Class<KeyType> keyTypeClass;
    /**
     * NameType fields count that has annotations
     */
    protected Field[] fields = null;

    /**
     * Instantiates a new abstract column family dao.
     */
    public AbstractIteratingColumnFamilyDao(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator,
                                            final Class<KeyType> keyTypeClass, final Class<NameType> nameClass, final Class<ValueType> valueClass,
                                            final String columnFamilyName, String comparatorAlias) {
        this.keySpace = new HectorManager().getKeyspace(clusterName, keyspaceConfigurator, columnFamilyName, false, comparatorAlias);
        this.keyTypeClass = keyTypeClass;
        this.nameClass = nameClass;
        this.valueClass = valueClass;
        this.columnFamilyName = columnFamilyName;
        fields = CompositeComponentProcessor.checkSetComposite(nameClass);
    }

    /**
     * Save.
     *
     * @param key   the key
     * @param name  the column name
     * @param value the column value
     */
    public void save(KeyType key, NameType name, ValueType value) {

        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass));

        if (fields != null) {
            Composite comp = marshalComposite(name);

            HColumn<Object, ValueType> column = (HColumn<Object, ValueType>) HFactory.createColumn(comp, value, SerializerTypeInferer.getSerializer(
                comp), SerializerTypeInferer.getSerializer(value));

            mutator.addInsertion(key, columnFamilyName, column);
        } else {

            HColumn<NameType, ValueType> column = (HColumn<NameType, ValueType>) HFactory.createColumn(name, value,
                SerializerTypeInferer.getSerializer(name), SerializerTypeInferer.getSerializer(value));

            mutator.addInsertion(key, columnFamilyName, column);
        }

        mutator.execute();
    }

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key) {
        return find(key, null, null);
    }

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, boolean reverse) {
        return find(key, null, null, reverse);
    }

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, NameType start, NameType end) {
        return find(key, start, end, false);
    }

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, NameType start, NameType end, boolean reverse) {
        SliceQuery<KeyType, Object, byte[]> query;

        if (fields != null) {
            query = HFactory.createSliceQuery(keySpace, (Serializer<KeyType>) SerializerTypeInferer.getSerializer(keyTypeClass),
                SerializerTypeInferer.getSerializer(Composite.class), BytesArraySerializer.get());
            query.setColumnFamily(columnFamilyName);
            query.setKey(key);

            Composite compStart = (start != null) ? marshalComposite(start) : null;
            Composite compEnd = (end != null) ? marshalComposite(end) : null;

            return new ColumnIterator<KeyType, NameType, ValueType>(query, nameClass, valueClass, compStart, compEnd, reverse);
        } else {
            query = HFactory.createSliceQuery(keySpace, (Serializer<KeyType>) SerializerTypeInferer.getSerializer(keyTypeClass),
                SerializerTypeInferer.getSerializer(nameClass), BytesArraySerializer.get());
            query.setColumnFamily(columnFamilyName);
            query.setKey(key);
            return new ColumnIterator<KeyType, NameType, ValueType>(query, nameClass, valueClass, start, end, reverse);
        }
    }

    /**
     * Delete.
     */
    public void delete(KeyType key) {
        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass));
        mutator.delete(key, columnFamilyName, null, SerializerTypeInferer.getSerializer(keyTypeClass));
    }

    /**
     * Delete.
     */
    public void deleteColumn(KeyType key, NameType name) {
        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(keyTypeClass));
        if (fields != null) {
            Composite comp = marshalComposite(name);
            mutator.delete(key, columnFamilyName, comp, SerializerTypeInferer.getSerializer(Composite.class));
        } else {
            mutator.delete(key, columnFamilyName, name, SerializerTypeInferer.getSerializer(nameClass));
        }
    }

    /**
     * Gets the keys.
     *
     * @return the keys
     */
    public Set<KeyType> getKeys() {
        int rows = 0;
        int pagination = 50;
        Set<KeyType> rowKeys = new HashSet<KeyType>();

        Row<Object, KeyType, byte[]> lastRow = null;

        do {
            RangeSlicesQuery<Object, KeyType, byte[]> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keySpace,
                SerializerTypeInferer.getSerializer(keyTypeClass), (Serializer<KeyType>) StringSerializer.get(), BytesArraySerializer.get());
            rangeSlicesQuery.setColumnFamily(columnFamilyName);
            if (lastRow != null) {
                rangeSlicesQuery.setKeys(lastRow.getKey(), "");
            } else {
                rangeSlicesQuery.setKeys(null, null);
            }
            rangeSlicesQuery.setReturnKeysOnly();
            rangeSlicesQuery.setRange(null, null, false, keyTypeClass.getDeclaredFields().length);
            rangeSlicesQuery.setRowCount(pagination);
            QueryResult<OrderedRows<Object, KeyType, byte[]>> result = rangeSlicesQuery.execute();
            OrderedRows<Object, KeyType, byte[]> orderedRows = result.get();
            rows = orderedRows.getCount();

            for (Row<Object, KeyType, byte[]> row : orderedRows) {
                if (!row.getColumnSlice().getColumns().isEmpty()) {
                    rowKeys.add((KeyType) row.getKey());
                }
            }

            lastRow = orderedRows.peekLast();
        } while (rows == pagination);

        return rowKeys;
    }

    /**
     * Contains.
     */
    public boolean containsKey(KeyType key) {
        RangeSlicesQuery<Object, String, byte[]> rangeSlicesQuery = HFactory.createRangeSlicesQuery(keySpace, SerializerTypeInferer.getSerializer(
            keyTypeClass), StringSerializer.get(), BytesArraySerializer.get());
        rangeSlicesQuery.setColumnFamily(columnFamilyName);
        rangeSlicesQuery.setKeys(key, key);
        rangeSlicesQuery.setReturnKeysOnly();
        rangeSlicesQuery.setRange("", "", false, 1);
        rangeSlicesQuery.setRowCount(1);
        QueryResult<OrderedRows<Object, String, byte[]>> result = rangeSlicesQuery.execute();
        OrderedRows<Object, String, byte[]> orderedRows = result.get();

        return (!orderedRows.getList().isEmpty() && !orderedRows.getByKey(key).getColumnSlice().getColumns().isEmpty());
    }

    protected Composite marshalComposite(NameType name) {

        try {

            Composite comp = new Composite();

            for (int i = 0;i < fields.length;i++) {
                Object obj = fields[i].get(name);

                comp.addComponent(obj, SerializerTypeInferer.getSerializer(fields[i].getType()), fields[i].getAnnotation(CompositeComponent.class)
                                                                                                          .type().name(),
                    AbstractComposite.ComponentEquality.EQUAL);
            }

            return comp;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
