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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.config.HectorHelper;
import com.savoirtech.hecate.core.config.HectorManager;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedSuperRows;
import me.prettyprint.hector.api.beans.SuperRow;
import me.prettyprint.hector.api.beans.SuperRows;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSuperSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

public abstract class AbstractSuperColumnFamilyDao<SKeyType, ST, KeyType, T> {

    /**
     * The key spacee
     */
    protected final Keyspace keySpace;
    /**
     * The column family name.
     */
    protected final String columnFamilyName;
    /**
     * The super key type class.
     */
    private final Class<SKeyType> superKeyTypeClass;
    /**
     * The key type class.
     */
    private final Class<KeyType> keyTypeClass;
    /**
     * The persistent class.
     */
    private final Class<T> persistentClass;
    /**
     * The super persistent class.
     */
    private final Class<ST> superPersistentClass;

    /**
     * Instantiates a new abstract column family dao.
     */
    public AbstractSuperColumnFamilyDao(final String clusterName, final CassandraKeyspaceConfigurator keyspaceConfigurator,
                                        final Class<SKeyType> superKeyTypeClass, final Class<ST> superPersistentClass,
                                        final Class<KeyType> keyTypeClass, final Class<T> persistentClass, final String columnFamilyName,
                                        String comparatorAlias) {
        this.keySpace = new HectorManager().getKeyspace(clusterName, keyspaceConfigurator, columnFamilyName, true, comparatorAlias);

        this.superKeyTypeClass = superKeyTypeClass;
        this.keyTypeClass = keyTypeClass;
        this.columnFamilyName = columnFamilyName;
        this.superPersistentClass = superPersistentClass;
        this.persistentClass = persistentClass;
    }

    /**
     * Save.
     *
     * @param superKey the super key
     * @param modelMap the model map
     */
    public void save(SKeyType superKey, Map<String, T> modelMap) {

        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(superKeyTypeClass));

        for (String key : modelMap.keySet()) {
            T t = modelMap.get(key);
            List<HColumn<String, Object>> columns = HectorHelper.getObjectColumns(t);
            HSuperColumn<Object, String, Object> superColumn = HFactory.createSuperColumn(key, columns, SerializerTypeInferer.getSerializer(
                superKeyTypeClass), StringSerializer.get(), ObjectSerializer.get());

            mutator.addInsertion(superKey, columnFamilyName, superColumn);
        }
        mutator.execute();
    }

    public boolean containsKey(final SKeyType superKey) {

        MultigetSuperSliceQuery<Object, Object, String, byte[]> multigetSuperSliceQuery = HFactory.createMultigetSuperSliceQuery(keySpace,
            SerializerTypeInferer.getSerializer(superKeyTypeClass), SerializerTypeInferer.getSerializer(keyTypeClass), StringSerializer.get(),
            BytesArraySerializer.get());

        multigetSuperSliceQuery.setColumnFamily(columnFamilyName);
        multigetSuperSliceQuery.setKeys(superKey);

        multigetSuperSliceQuery.setRange("", "", false, Integer.MAX_VALUE);

        QueryResult<SuperRows<Object, Object, String, byte[]>> result = multigetSuperSliceQuery.execute();

        try {
            return (!result.get().getByKey(superKey).getSuperSlice().getSuperColumns().isEmpty());
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Find.
     *
     * @param superKey    the super key
     * @param columnNames Optional the column names
     * @return the t
     */
    public ST find(final SKeyType superKey, final String[] columnNames) {

        List<HSuperColumn<Object, String, byte[]>> superColumns = null;

        SuperSliceQuery<Object, Object, String, byte[]> superColumnQuery = HFactory.createSuperSliceQuery(keySpace,
            SerializerTypeInferer.getSerializer(superKeyTypeClass), SerializerTypeInferer.getSerializer(keyTypeClass), StringSerializer.get(),
            BytesArraySerializer.get());
        superColumnQuery.setColumnFamily(columnFamilyName).setKey(superKey);
        if (columnNames == null || (columnNames.length > 0 && "All".equals(columnNames[0]))) {
            superColumnQuery.setRange("", "", false, 50);
        } else {
            superColumnQuery.setColumnNames(columnNames);
        }

        QueryResult<SuperSlice<Object, String, byte[]>> result = superColumnQuery.execute();

        try {
            superColumns = result.get().getSuperColumns();

            if (superColumns.isEmpty()) {
                return null;
            }
        } catch (Exception e) {
            return null;
        }

        try {
            ST st = superPersistentClass.newInstance();
            T t = persistentClass.newInstance();

            HectorHelper.populateSuperEntity(st, t, superKey, superColumns);
            return st;
        } catch (Exception e) {
            throw new RuntimeException("Error creating persistent class", e);
        }
    }

    /**
     * Find super items.
     *
     * @param superKeys   the super keys
     * @param columnNames Optional the column names
     * @return the map
     */
    public Map<String, ST> findItems(final List<SKeyType> superKeys, final String[] columnNames) {

        Map<String, ST> result = new HashMap<String, ST>();
        for (SKeyType superKey : superKeys) {
            result.put((String) superKey, find(superKey, columnNames));
        }

        return result;
    }

    /**
     * Delete.
     */
    public void delete(SKeyType superKey) {
        Mutator<Object> mutator = HFactory.createMutator(keySpace, SerializerTypeInferer.getSerializer(superKeyTypeClass));
        mutator.delete(superKey, columnFamilyName, null, SerializerTypeInferer.getSerializer(superKeyTypeClass));
    }

    /**
     * Gets the keys.
     *
     * @return the keys
     */
    public Set<String> getKeys() {
        int rows = 0;
        int pagination = 50;
        Set<String> rowKeys = new HashSet<String>();

        SuperRow<Object, String, String, byte[]> lastRow = null;

        do {
            RangeSuperSlicesQuery<Object, String, String, byte[]> rangeSuperSliceQuery = HFactory.createRangeSuperSlicesQuery(keySpace,
                SerializerTypeInferer.getSerializer(superKeyTypeClass), StringSerializer.get(), StringSerializer.get(), BytesArraySerializer.get());

            rangeSuperSliceQuery.setColumnFamily(columnFamilyName);
            if (lastRow != null) {
                rangeSuperSliceQuery.setKeys(lastRow.getKey(), "");
            } else {
                rangeSuperSliceQuery.setKeys("", "");
            }
            rangeSuperSliceQuery.setRange("", "", false, 2);
            rangeSuperSliceQuery.setRowCount(pagination);

            QueryResult<OrderedSuperRows<Object, String, String, byte[]>> result = rangeSuperSliceQuery.execute();
            OrderedSuperRows<Object, String, String, byte[]> orderedSuperRows = result.get();
            rows = orderedSuperRows.getCount();

            for (SuperRow<Object, String, String, byte[]> row : orderedSuperRows) {
                if (!row.getSuperSlice().getSuperColumns().isEmpty()) {
                    rowKeys.add((String) row.getKey());
                    lastRow = orderedSuperRows.getList().get(rows - 1);
                }
            }
        } while (rows == pagination);

        return rowKeys;
    }
}
