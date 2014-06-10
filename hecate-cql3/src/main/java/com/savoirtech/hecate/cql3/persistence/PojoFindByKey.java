package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoFindByKey extends PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKey.class);

    private final PojoMapping pojoMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoFindByKey(Session session, PojoMapping mapping) {
        super(session, createSelect(mapping), mapping);
        this.pojoMapping = mapping;
    }

    private static <P> Select.Where createSelect(PojoMapping mapping) {
        final Select.Where where = pojoSelect(mapping)
                .from(mapping.getTableName())
                .where(QueryBuilder.eq(mapping.getIdentifierMapping().getFacetMetadata().getColumnName(), QueryBuilder.bindMarker()));

        LOGGER.info("{}.findByKey(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), where);
        return where;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Object find(Object identifier, QueryContext context) {
        return find(identifier, pojoMapping.getPojoMetadata().newPojo(), context);
    }

    public Object find(Object identifier, Object pojo, QueryContext context) {
        return one(pojo, executeWithArgs(pojoMapping.getIdentifierMapping().getColumnHandler().getWhereClauseValue(identifier)), context);
    }
}
