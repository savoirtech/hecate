package com.savoirtech.hecate.cql3.schema;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;

public interface SchemaVerifier {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <P> void verifySchema(Session session, String tableName, PojoDescriptor<P> descriptor);
}
