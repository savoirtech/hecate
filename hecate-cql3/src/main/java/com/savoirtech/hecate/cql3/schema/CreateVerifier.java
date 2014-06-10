package com.savoirtech.hecate.cql3.schema;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateVerifier implements SchemaVerifier {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateVerifier.class);

//----------------------------------------------------------------------------------------------------------------------
// SchemaVerifier Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void verifySchema(Session session, PojoMapping mapping) {
        final StringBuilder cql = new StringBuilder();
        cql.append("CREATE TABLE IF NOT EXISTS ");
        cql.append(session.getLoggedKeyspace());
        cql.append(".");
        cql.append(mapping.getPojoMetadata().getTableName());
        cql.append(" (");
        cql.append(StringUtils.join(mapping.getFacetMappings(), ", "));
        cql.append(")");
        LOGGER.info("Creating table for type {}: {}", mapping.getPojoMetadata().getPojoType().getCanonicalName(), cql);
        session.execute(cql.toString());
    }
}
