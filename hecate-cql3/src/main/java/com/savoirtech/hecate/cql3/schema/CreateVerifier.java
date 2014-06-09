package com.savoirtech.hecate.cql3.schema;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateVerifier implements SchemaVerifier {
    //----------------------------------------------------------------------------------------------------------------------
// SchemaVerifier Implementation
//----------------------------------------------------------------------------------------------------------------------
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateVerifier.class);

    @Override
    public <P> void verifySchema(Session session, String tableName, PojoDescriptor<P> descriptor) {
        final StringBuilder cql = new StringBuilder();
        cql.append("CREATE TABLE IF NOT EXISTS ");
        cql.append(session.getLoggedKeyspace());
        cql.append(".");
        cql.append(tableName);
        cql.append(" (");
        cql.append(StringUtils.join(descriptor.getFacetMappings(), ", "));
        cql.append(")");
        LOGGER.info("Creating table for type {}: {}", descriptor.getPojoType(), cql);
        session.execute(cql.toString());
    }
}
