package com.savoirtech.hecate.cql3.schema;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;

public interface SchemaVerifier {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    void verifySchema(Session session, PojoMapping mapping);
}
