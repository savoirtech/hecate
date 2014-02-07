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

package com.savoirtech.hecate.core.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.dao.ColumnFamilyDao;
import com.savoirtech.hecate.core.dao.PojoObjectGraphDao;

public class DaoPool<V> {

    private final Class<V> daoClass;
    private final CassandraKeyspaceConfigurator configurator;
    private final String cluster;

    private Map<String, ColumnFamilyDao> daoMap = new ConcurrentHashMap<>();

    public DaoPool(String cluster, CassandraKeyspaceConfigurator configurator, Class<V> daoClass) {
        this.daoClass = daoClass;
        this.configurator = configurator;
        this.cluster = cluster;
    }

    @SuppressWarnings("unchecked")
    public <K, T> ColumnFamilyDao<K, T> getPojoDao(Class<K> keyClass, Class<T> storageClass, String columnFamily, String comparatorAlias) {
        ColumnFamilyDao<K, T> dao = null;
        if (configurator == null) {
            throw new IllegalArgumentException("Configuration cannot be null");
        }

        final String daoKey = configurator.getKeyspace() + ":::" + columnFamily + ":::" + storageClass.getName();
        dao = (ColumnFamilyDao<K, T>) daoMap.get(daoKey);
        if (dao == null) {
            dao = new PojoObjectGraphDao<>(cluster, configurator, keyClass, storageClass, columnFamily, comparatorAlias,this);
            daoMap.put(daoKey, dao);
        }
        return dao;
    }
}
