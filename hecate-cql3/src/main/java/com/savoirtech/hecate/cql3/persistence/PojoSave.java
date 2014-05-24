package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.cql3.meta.ColumnDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoSave<P> extends PojoPersistenceStatement<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoSave.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoSave(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
        super(session, createInsert(table, pojoDescriptor), pojoDescriptor);
    }

    private static <P> Insert createInsert(String table, PojoDescriptor<P> pojoDescriptor) {
        final Insert insert = QueryBuilder.insertInto(table);
        for (ColumnDescriptor columnDescriptor : pojoDescriptor.getColumns()) {
            insert.value(columnDescriptor.getColumnName(), QueryBuilder.bindMarker());
        }
        LOGGER.info("{}.save():: {}", pojoDescriptor.getPojoType().getSimpleName(), insert);
        return insert;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(P pojo) {
        executeWithList(cassandraValues(pojo, allColumns()));
    }
}
