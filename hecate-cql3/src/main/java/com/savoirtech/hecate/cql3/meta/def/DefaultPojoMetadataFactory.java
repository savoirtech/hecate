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

package com.savoirtech.hecate.cql3.meta.def;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;
import com.savoirtech.hecate.cql3.value.field.FieldFacetProvider;
import org.apache.commons.lang3.Validate;

import java.util.Map;

public class DefaultPojoMetadataFactory implements PojoMetadataFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Map<Class<?>, PojoMetadata> pojoMetadatas;
    private FacetProvider facetProvider = new FieldFacetProvider();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMetadataFactory() {
        pojoMetadatas = new MapMaker().makeMap();
    }

    public DefaultPojoMetadataFactory(int concurrencyLevel) {
        pojoMetadatas = new MapMaker().concurrencyLevel(concurrencyLevel).makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMetadataFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoMetadata getPojoMetadata(Class<?> pojoType) {
        PojoMetadata pojoMetadata = pojoMetadatas.get(pojoType);
        if (pojoMetadata == null) {
            pojoMetadata = new PojoMetadata(pojoType);
            for (Facet facet : facetProvider.getFacets(pojoType)) {
                pojoMetadata.addFacet(new FacetMetadata(facet));
            }
            pojoMetadatas.put(pojoType, pojoMetadata);
        }
        Validate.isTrue(pojoMetadata.getIdentifierFacet() != null, "Invalid POJO type %s (no identifier found).", pojoType.getCanonicalName());
        return pojoMetadata;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setFacetProvider(FacetProvider facetProvider) {
        this.facetProvider = facetProvider;
    }
}
