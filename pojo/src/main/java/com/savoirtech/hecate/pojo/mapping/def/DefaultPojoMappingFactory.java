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
import com.savoirtech.hecate.annotation.*;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.def.DefaultConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.field.FieldFacetProvider;
import com.savoirtech.hecate.pojo.mapping.*;
import com.savoirtech.hecate.pojo.mapping.column.*;
import com.savoirtech.hecate.pojo.mapping.name.NamingStrategy;
import com.savoirtech.hecate.pojo.mapping.name.def.DefaultNamingStrategy;
import com.savoirtech.hecate.pojo.util.GenericType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultPojoMappingFactory implements PojoMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPojoMappingFactory.class);
    private final FacetProvider facetProvider;
    private final ConverterRegistry converterRegistry;
    private final Map<Class<?>, Function<GenericType, ColumnType>> columnTypeOverrides = new HashMap<>();

    private final LoadingCache<Pair<Class<?>, String>, PojoMapping<?>> mappingCache = CacheBuilder.newBuilder().build(new MappingCacheLoader());
    private final PojoMappingVerifier verifier;
    private final NamingStrategy namingStrategy;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMappingFactory() {
        this(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry(), null, new DefaultNamingStrategy());
    }

    public DefaultPojoMappingFactory(PojoMappingVerifier verifier) {
        this(new FieldFacetProvider(), DefaultConverterRegistry.defaultRegistry(), verifier, new DefaultNamingStrategy());
    }

    public DefaultPojoMappingFactory(FacetProvider facetProvider, ConverterRegistry converterRegistry, PojoMappingVerifier verifier, NamingStrategy namingStrategy) {
        this.facetProvider = facetProvider;
        this.converterRegistry = converterRegistry;
        this.verifier = verifier;
        this.namingStrategy = namingStrategy;
        columnTypeOverrides.put(Set.class, facetType -> new SetColumnType());
        columnTypeOverrides.put(List.class, facetType -> new ListColumnType());
        columnTypeOverrides.put(Map.class, facetType -> new MapColumnType(converterRegistry.getRequiredConverter(facetType.getMapKeyType())));
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> PojoMapping<P> createPojoMapping(Class<P> pojoClass) {
        return createPojoMapping(pojoClass, namingStrategy.getTableName(pojoClass));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoMapping<P> createPojoMapping(Class<P> pojoClass, String tableName) {
        return (PojoMapping<P>) mappingCache.getUnchecked(new ImmutablePair<>(pojoClass, tableName));
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private void addIdMappings(Facet idFacet, List<ScalarFacetMapping> mappings) {
        final Converter converter = converterRegistry.getConverter(idFacet.getType());
        if (converter == null) {
            addEmbeddedMappings(idFacet, false, mappings, facet -> {
                if (!facet.hasAnnotation(PartitionKey.class) && !facet.hasAnnotation(ClusteringColumn.class)) {
                    throw new HecateException("Sub-facet %s found on @Id facet %s does not contain @PrimaryKey or @ClusteringColumn annotation.", facet.getName(), idFacet.getName());
                }
            });
        } else {
            LOGGER.debug("Creating scalar @Id mapping for {}...", idFacet.getName());
            mappings.add(new ScalarFacetMapping(idFacet, namingStrategy.getColumnName(idFacet), SimpleColumnType.INSTANCE, converter));
        }
    }

    private void addEmbeddedMappings(Facet parentFacet, boolean allowNullParent, List<? super ScalarFacetMapping> target, Consumer<Facet> verifier) {
        LOGGER.debug("Creating embedded mappings for {}...", parentFacet.getName());
        if (allowNullParent) {
            target.add(new ScalarFacetMapping(parentFacet, namingStrategy.getColumnName(parentFacet), EmbeddedColumnType.INSTANCE, Converter.NULL_CONVERTER));
        }
        List<Facet> subFacets = parentFacet.subFacets(!allowNullParent);
        for (Facet subFacet : subFacets) {
            verifier.accept(subFacet);
            LOGGER.debug("Creating embedded mapping for sub-facet {}...", subFacet.getName());
            target.add(new ScalarFacetMapping(subFacet, namingStrategy.getColumnName(subFacet), SimpleColumnType.INSTANCE, converterRegistry.getRequiredConverter(subFacet.getType())));
        }
    }

    private void addMappings(Facet facet, List<FacetMapping> mappings) {
        GenericType facetType = facet.getType();
        final ColumnType<?, ?> columnType = createColumnType(facet);
        final Converter converter = converterRegistry.getConverter(facet.getType().getElementType());
        if (facet.hasAnnotation(Embedded.class)) {
            addEmbeddedMappings(facet, true, mappings, Facet::getName);
        } else if (converter != null) {
            LOGGER.debug("Creating scalar mapping for {}...", facet.getName());
            mappings.add(new ScalarFacetMapping(facet, namingStrategy.getColumnName(facet), columnType, converter));
        } else {
            LOGGER.debug("Creating reference mapping for {}...", facet.getName());
            final String tableName = namingStrategy.getReferenceTableName(facet);
            mappings.add(new ReferenceFacetMapping(facet, namingStrategy.getColumnName(facet), columnType, createPojoMapping(facetType.getElementType().getRawType(), tableName)));
        }
    }

    private ColumnType<?, ?> createColumnType(Facet facet) {
        final GenericType facetType = facet.getType();
        Function<GenericType, ColumnType> override = columnTypeOverrides.get(facetType.getRawType());
        if (override != null) {
            return override.apply(facetType);
        } else if (facetType.getRawType().isArray()) {
            return new ArrayColumnType();
        } else if (facet.hasAnnotation(Embedded.class)) {
            return EmbeddedColumnType.INSTANCE;
        } else {
            return SimpleColumnType.INSTANCE;
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class MappingCacheLoader extends CacheLoader<Pair<Class<?>, String>, PojoMapping<?>> {
        @Override
        public PojoMapping<?> load(Pair<Class<?>, String> key) throws Exception {
            LOGGER.debug("Creating mapping for class {} in table {}.", key.getKey().getCanonicalName(), key.getValue());
            final List<ScalarFacetMapping> idMappings = new LinkedList<>();
            final List<FacetMapping> simpleMappings = new LinkedList<>();
            List<Facet> facets = facetProvider.getFacets(key.getLeft());
            for (Facet facet : facets) {
                if (facet.hasAnnotation(Id.class)) {
                    addIdMappings(facet, idMappings);
                } else {
                    addMappings(facet, simpleMappings);
                }
            }
            PojoMapping<?> mapping = new PojoMapping<>(key.getLeft(), key.getRight(), idMappings, simpleMappings);
            if (verifier != null) {
                verifier.verify(mapping);
            }
            return mapping;
        }
    }
}
