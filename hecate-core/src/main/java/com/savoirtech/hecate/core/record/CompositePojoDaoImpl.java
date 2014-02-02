package com.savoirtech.hecate.core.record;

import com.savoirtech.hecate.core.config.CassandraKeyspaceConfigurator;
import com.savoirtech.hecate.core.utils.AbstractIteratingRecordDao;

public class CompositePojoDaoImpl extends AbstractIteratingRecordDao implements CompositePojoDao {
    //----------------------------------------------------------------------------------------------------------------------
    // Constructors
    //----------------------------------------------------------------------------------------------------------------------

    public CompositePojoDaoImpl(String clusterName, CassandraKeyspaceConfigurator keyspaceConfigurator, String columnFamilyName,
                                String comparatorAlias) {
        super(clusterName, keyspaceConfigurator, String.class, CompositeColumnIdentifier.class, String.class, columnFamilyName, comparatorAlias);
    }
}
