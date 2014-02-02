package com.savoirtech.hecate.core.utils;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.dao.ColumnFamilyDao;

public class AnnotatedColumnImpl extends AbstractAnnotatedColumnFamilyDao implements ColumnFamilyDao {
    /**
     * Instantiates a new abstract column family dao.
     */
    public AnnotatedColumnImpl(String clusterName, CassandraKeyspaceConfigurator keyspaceConfigurator, Class keyClass, Class typeClass,
                               String columnFamilyName, String comparatorAlias) {
        super(clusterName, keyspaceConfigurator, keyClass, typeClass, columnFamilyName, comparatorAlias);
    }
}
