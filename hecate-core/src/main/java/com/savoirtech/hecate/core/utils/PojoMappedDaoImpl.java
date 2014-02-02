package com.savoirtech.hecate.core.utils;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.dao.ColumnFamilyDao;

public class PojoMappedDaoImpl<K, T> extends AbstractPojoMappedColumnFamilyDao<K, T> implements ColumnFamilyDao<K, T> {
    /**
     * Instantiates a new abstract column family dao.
     */
    public PojoMappedDaoImpl(String clusterName, CassandraKeyspaceConfigurator keyspaceConfigurator, Class<K> keyClass, Class<T> typeClass,
                             String columnFamilyName, String comparatorAlias) {
        super(clusterName, keyspaceConfigurator, keyClass, typeClass, columnFamilyName, comparatorAlias);
    }
}
