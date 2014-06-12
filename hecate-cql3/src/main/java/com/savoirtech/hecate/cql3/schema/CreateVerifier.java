/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.schema;

import com.datastax.driver.core.Session;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
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
        cql.append(mapping.getTableName());
        cql.append(" (");
        cql.append(StringUtils.join(mapping.getFacetMappings(), ", "));
        cql.append(")");
        LOGGER.info("Creating table for type {}: {}", mapping.getPojoMetadata().getPojoType().getCanonicalName(), cql);
        session.execute(cql.toString());

        createIndexes(session, mapping);
    }

    private void createIndexes(Session session, PojoMapping mapping) {
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            if (facetMapping.getFacetMetadata().isIndexed()) {

                final String cql = String.format("CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s)",
                        facetMapping.getFacetMetadata().getIndexName(),
                        session.getLoggedKeyspace(),
                        mapping.getTableName(),
                        facetMapping.getFacetMetadata().getColumnName());
                LOGGER.info("Creating index: {}", cql);
                session.execute(cql);
            }
        }
    }
}
