package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoDelete<P> extends PojoPersistenceStatement<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static Logger LOGGER = LoggerFactory.getLogger(PojoDelete.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDelete(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
        super(session, createDelete(table, pojoDescriptor), pojoDescriptor);
    }

    private static <P> Delete.Where createDelete(String table, PojoDescriptor<P> pojoDescriptor) {
        final Delete.Where delete = delete().from(table).where(eq(pojoDescriptor.getIdentifierColumn().getColumnName(), bindMarker()));
        LOGGER.info("{}.delete():: {}", pojoDescriptor.getPojoType().getSimpleName(), delete);
        return delete;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(P pojo) {
        executeWithArgs(cassandraValue(pojo, identifierColumn()));
    }
}
