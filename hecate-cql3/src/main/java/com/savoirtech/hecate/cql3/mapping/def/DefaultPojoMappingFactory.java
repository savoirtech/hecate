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

package com.savoirtech.hecate.cql3.mapping.def;

import com.datastax.driver.core.Session;
import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.handler.ColumnHandlerFactory;
import com.savoirtech.hecate.cql3.handler.def.DefaultColumnHandlerFactory;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.meta.def.DefaultPojoMetadataFactory;
import com.savoirtech.hecate.cql3.schema.CreateVerifier;
import com.savoirtech.hecate.cql3.schema.SchemaVerifier;

import java.util.Map;

public class DefaultPojoMappingFactory implements PojoMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Session session;
    private PojoMetadataFactory pojoMetadataFactory = new DefaultPojoMetadataFactory();
    private ColumnHandlerFactory columnHandlerFactory = new DefaultColumnHandlerFactory();
    private SchemaVerifier schemaVerifier = new CreateVerifier();
    private final Map<String, PojoMapping> pojoMappings;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMappingFactory(Session session) {
        this.session = session;
        this.pojoMetadataFactory = new DefaultPojoMetadataFactory();
        final DefaultColumnHandlerFactory handlerFactory = new DefaultColumnHandlerFactory();
        handlerFactory.setPojoMetadataFactory(pojoMetadataFactory);
        this.columnHandlerFactory = handlerFactory;
        this.pojoMappings = new MapMaker().makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public PojoMapping getPojoMapping(Class<?> pojoType, String tableName) {
        final String key = key(pojoType, tableName);
        PojoMapping pojoMapping = pojoMappings.get(key);
        if (pojoMapping == null) {
            PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(pojoType);
            pojoMapping = new PojoMapping(pojoMetadata, tableName);
            for (FacetMetadata facet : pojoMetadata.getFacets().values()) {
                pojoMapping.addFacet(new FacetMapping(facet, columnHandlerFactory.getColumnHandler(facet)));
            }
            schemaVerifier.verifySchema(session, pojoMapping);
            pojoMappings.put(key, pojoMapping);
            pojoMappings.put(key(pojoType, pojoMapping.getTableName()), pojoMapping);
        }
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setColumnHandlerFactory(ColumnHandlerFactory columnHandlerFactory) {
        this.columnHandlerFactory = columnHandlerFactory;
    }

    public void setPojoMetadataFactory(PojoMetadataFactory pojoMetadataFactory) {
        this.pojoMetadataFactory = pojoMetadataFactory;
    }

    public void setSchemaVerifier(SchemaVerifier schemaVerifier) {
        this.schemaVerifier = schemaVerifier;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private String key(Class<?> pojoType, String tableName) {
        return pojoType.getCanonicalName() + "@" + tableName;
    }
}
