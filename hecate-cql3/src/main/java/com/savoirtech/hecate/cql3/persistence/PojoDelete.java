package com.savoirtech.hecate.cql3.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PojoDelete<P> { //extends PojoPersistenceStatement<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static Logger LOGGER = LoggerFactory.getLogger(PojoDelete.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

//    public PojoDelete(Session session, String table, PojoDescriptor<P> pojoDescriptor) {
//        super(session, createDelete(table, pojoDescriptor), pojoDescriptor);
//    }
//
//    private static <P> Delete.Where createDelete(String table, PojoDescriptor<P> pojoDescriptor) {
//        final Delete.Where delete = delete().from(table).where(eq(pojoDescriptor.getIdentifierMapping().getColumnName(), bindMarker()));
//        LOGGER.info("{}.delete(): {}", pojoDescriptor.getPojoType().getSimpleName(), delete);
//        return delete;
//    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

//    public void execute(P pojo) {
//        executeWithArgs(cassandraValue(pojo, identifierMapping()));
//    }
}
