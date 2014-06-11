package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoDelete extends PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoDelete.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDelete(Session session, PojoMapping mapping) {
        super(session, createDelete(mapping), mapping);
    }

    private static Delete.Where createDelete(PojoMapping mapping) {
        final Delete.Where delete = delete().from(mapping.getTableName()).where(in(mapping.getIdentifierMapping().getFacetMetadata().getColumnName(), bindMarker()));
        LOGGER.info("{}.delete(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), delete);
        return delete;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(Iterable<Object> keys) {
        executeWithArgs(cassandraIdentifiers(keys));
    }
}
