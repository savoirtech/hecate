/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.mapping.verify;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingVerifier;
import com.savoirtech.hecate.pojo.mapping.name.NamingStrategy;
import com.savoirtech.hecate.pojo.mapping.name.def.DefaultNamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSchemaVerifier implements PojoMappingVerifier {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Logger LOGGER = LoggerFactory.getLogger(CreateSchemaVerifier.class);

    private final Session session;
    private final NamingStrategy namingStrategy;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CreateSchemaVerifier(Session session) {
        this(session, new DefaultNamingStrategy());
    }

    public CreateSchemaVerifier(Session session, NamingStrategy namingStrategy) {
        this.session = session;
        this.namingStrategy = namingStrategy;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingVerifier Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void verify(PojoMapping<?> pojoMapping) {
        LOGGER.info("Creating schema for {}...", pojoMapping);
        createTable(pojoMapping);
        createIndexes(pojoMapping);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    protected void createIndexes(PojoMapping<?> pojoMapping) {
        pojoMapping.getSimpleMappings().forEach(mapping -> {
            if (mapping.getFacet().isIndexed()) {
                executeStatement(SchemaBuilder.createIndex(namingStrategy.getIndexName(mapping.getFacet())).ifNotExists().onTable(pojoMapping.getTableName()).andColumn(mapping.getColumnName()));
            }
        });
    }

    private void executeStatement(SchemaStatement statement) {
        LOGGER.info("\n{}\n", statement.getQueryString());
        session.execute(statement);
    }

    protected Create createTable(PojoMapping<?> pojoMapping) {
        Create create = SchemaBuilder.createTable(pojoMapping.getTableName());
        pojoMapping.getIdMappings().forEach(mapping -> {
            String columnName = mapping.getColumnName();
            if (mapping.getFacet().hasAnnotation(PartitionKey.class) || mapping.getFacet().hasAnnotation(Id.class)) {
                LOGGER.debug("Adding partition key column {}...", columnName);
                create.addPartitionKey(columnName, mapping.getDataType());
            } else if (mapping.getFacet().hasAnnotation(ClusteringColumn.class)) {
                LOGGER.debug("Adding clustering column {}...", columnName);
                create.addClusteringColumn(columnName, mapping.getDataType());
            }
        });
        pojoMapping.getSimpleMappings().forEach(mapping -> {
            LOGGER.debug("Adding simple column {}...", mapping.getColumnName());
            create.addColumn(mapping.getColumnName(), mapping.getDataType());
        });
        create.ifNotExists();
        executeStatement(create);
        return create;
    }
}
