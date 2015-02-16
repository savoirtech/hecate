/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;
import com.savoirtech.hecate.cql3.value.field.FieldFacetProvider;
import org.apache.commons.lang3.Validate;

public class DefaultPojoMetadataFactory implements PojoMetadataFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final LoadingCache<Class<?>, PojoMetadata> pojoMetadatas;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMetadataFactory() {
        this(new FieldFacetProvider());
    }

    public DefaultPojoMetadataFactory(final FacetProvider facetProvider) {
        this.pojoMetadatas = CacheBuilder.newBuilder().build(new PojoMetadataCacheLoader(facetProvider));
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMetadataFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoMetadata getPojoMetadata(Class<?> pojoType) {
        PojoMetadata pojoMetadata = pojoMetadatas.getUnchecked(pojoType);
        Validate.isTrue(pojoMetadata.getIdentifierFacet() != null, "Invalid POJO type %s (no identifier found).", pojoType.getCanonicalName());
        return pojoMetadata;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class PojoMetadataCacheLoader extends CacheLoader<Class<?>, PojoMetadata> {
        private final FacetProvider facetProvider;

        public PojoMetadataCacheLoader(FacetProvider facetProvider) {
            this.facetProvider = facetProvider;
        }

        @Override
        public PojoMetadata load(Class<?> pojoType) throws Exception {
            PojoMetadata pojoMetadata = new PojoMetadata(pojoType);
            for (Facet facet : facetProvider.getFacets(pojoType)) {
                pojoMetadata.addFacet(new FacetMetadata(facet));
            }
            return pojoMetadata;
        }
    }
}
