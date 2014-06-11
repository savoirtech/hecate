/*
 * Copyright (c) 2014. Savoir Technologies
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

package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.meta.PojoMetadata;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PojoMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<FacetMapping> facetMappings = new LinkedList<>();

    private final PojoMetadata pojoMetadata;
    private final String tableName;
    private FacetMapping identifierMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping(PojoMetadata pojoMetadata, String tableName) {
        this.pojoMetadata = pojoMetadata;
        this.tableName = tableName == null ? pojoMetadata.getDefaultTableName() : tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping getIdentifierMapping() {
        return identifierMapping;
    }

    public PojoMetadata getPojoMetadata() {
        return pojoMetadata;
    }

    public String getTableName() {
        return tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addFacet(FacetMapping facetMapping) {
        facetMappings.add(facetMapping);
        if (facetMapping.getFacetMetadata().isIdentifier()) {
            identifierMapping = facetMapping;
        }
    }

    public List<FacetMapping> getFacetMappings() {
        return Collections.unmodifiableList(facetMappings);
    }

    public boolean isCascading() {
        for (FacetMapping facetMapping : facetMappings) {
            if (facetMapping.getColumnHandler().isCascading()) {
                return true;
            }
        }
        return false;
    }
}
