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

package com.savoirtech.hecate.pojo.mapping.def;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.mapping.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMapping;
import com.savoirtech.hecate.pojo.mapping.PojoMappingFactory;
import com.savoirtech.hecate.pojo.mapping.column.*;
import com.savoirtech.hecate.pojo.mapping.element.ConverterElementHandler;
import com.savoirtech.hecate.pojo.mapping.element.ElementHandler;
import com.savoirtech.hecate.pojo.mapping.element.PojoElementHandler;
import com.savoirtech.hecate.pojo.util.GenericType;
import com.savoirtech.hecate.pojo.util.PojoUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class DefaultPojoMappingFactory implements PojoMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPojoMappingFactory.class);
    private final FacetProvider facetProvider;
    private final ConverterRegistry converterRegistry;
    private final Map<Class<?>, Function<Facet, ColumnType>> columnTypeOverrides = new HashMap<>();

    private LoadingCache<Pair<Class<?>,String>,PojoMapping<?>> mappingCache = CacheBuilder.newBuilder().build(new MappingCacheLoader());

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMappingFactory(FacetProvider facetProvider, ConverterRegistry converterRegistry) {
        this.facetProvider = facetProvider;
        this.converterRegistry = converterRegistry;

        columnTypeOverrides.put(Set.class, facet -> new SetColumnType(createElementHandler(facet, facet.getType().getSetElementType())));
        columnTypeOverrides.put(List.class, facet -> new ListColumnType(createElementHandler(facet, facet.getType().getListElementType())));
        columnTypeOverrides.put(Map.class, facet -> new MapColumnType(converterRegistry.getRequiredConverter(facet.getType().getMapKeyType()), createElementHandler(facet, facet.getType().getMapValueType())));
    }

    private ElementHandler createElementHandler(Facet facet, GenericType elementType) {
        Converter converter = converterRegistry.getConverter(elementType);
        if (converter == null) {
            Table table = facet.getAnnotation(Table.class);
            return new PojoElementHandler(createPojoMapping(elementType.getRawType(), table == null ? PojoUtils.getTableName(elementType.getRawType()) : table.value()));
        } else {
            return new ConverterElementHandler(converter);
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> PojoMapping<P> createPojoMapping(Class<P> pojoClass) {
        return createPojoMapping(pojoClass, PojoUtils.getTableName(pojoClass));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoMapping<P> createPojoMapping(Class<P> pojoClass, String tableName) {
        return (PojoMapping<P>)mappingCache.getUnchecked(new ImmutablePair<>(pojoClass, tableName));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private void addIdMappings(Facet idFacet, List<FacetMapping> mappings) {
        final Converter converter = converterRegistry.getConverter(idFacet.getType());
        if (converter == null) {
            List<Facet> subFacets = idFacet.subFacets();
            for (Facet subFacet : subFacets) {
                if (subFacet.hasAnnotation(PartitionKey.class) || subFacet.hasAnnotation(ClusteringColumn.class)) {
                    mappings.add(new FacetMapping(subFacet, new SimpleColumnType(new ConverterElementHandler(converterRegistry.getRequiredConverter(subFacet.getType())))));
                } else {
                    throw new HecateException("Sub-facet %s found on @Id facet %s does not contain @PrimaryKey or @ClusteringColumn annotation.", subFacet.getName(), idFacet.getName());
                }
            }
        } else {
            mappings.add(new FacetMapping(idFacet, new SimpleColumnType(new ConverterElementHandler(converter))));
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class MappingCacheLoader extends CacheLoader<Pair<Class<?>,String>,PojoMapping<?>> {
        @Override
        public PojoMapping<?> load(Pair<Class<?>, String> key) throws Exception {
            List<FacetMapping> mappings = new LinkedList<>();
            List<Facet> facets = facetProvider.getFacets(key.getLeft());
            for (Facet facet : facets) {
                GenericType facetType = facet.getType();
                if (facet.hasAnnotation(Id.class)) {
                    addIdMappings(facet, mappings);
                } else {
                    Function<Facet, ColumnType> override = columnTypeOverrides.get(facetType.getRawType());
                    if (override != null) {
                        mappings.add(new FacetMapping(facet, override.apply(facet)));
                    } else if (facetType.getRawType().isArray()) {
                        mappings.add(new FacetMapping(facet, new ArrayColumnType(createElementHandler(facet, facetType.getArrayElementType()))));
                    } else {
                        mappings.add(new FacetMapping(facet, new SimpleColumnType(createElementHandler(facet, facetType))));
                    }
                }
            }
            return new PojoMapping<>(key.getLeft(), key.getRight(), mappings);
        }
    }
}
