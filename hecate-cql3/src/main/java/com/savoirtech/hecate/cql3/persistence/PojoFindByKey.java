package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoFindByKey<P, K> extends PojoPersistenceStatement<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKey.class);

    private final PojoDescriptor<P> pojoDescriptor;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoFindByKey(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
        super(session, createSelect(table, pojoDescriptor), pojoDescriptor);
        this.pojoDescriptor = pojoDescriptor;
    }

    private static <P> Select.Where createSelect(String table, PojoDescriptor<P> pojoDescriptor) {
        final Select.Where where = pojoSelect(pojoDescriptor)
                .from(table)
                .where(QueryBuilder.eq(pojoDescriptor.getIdentifierColumn().getColumnName(), QueryBuilder.bindMarker()));

        LOGGER.info("{}.findByKey(): {}", pojoDescriptor.getPojoType().getSimpleName(), where);
        return where;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public P find(K key) {
        return one(executeWithArgs(identifierColumn().getMapping().rawCassandraValue(key)));
    }
}
