/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding.def;

import java.util.LinkedList;
import java.util.List;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Embedded;
import com.savoirtech.hecate.annotation.EmbeddedKey;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ElementBinding;
import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.PojoBinding;
import com.savoirtech.hecate.pojo.binding.PojoBindingFactory;
import com.savoirtech.hecate.pojo.binding.element.PojoElementBinding;
import com.savoirtech.hecate.pojo.binding.element.ScalarElementBinding;
import com.savoirtech.hecate.pojo.binding.facet.*;
import com.savoirtech.hecate.pojo.binding.key.composite.CompositeKeyBinding;
import com.savoirtech.hecate.pojo.binding.key.composite.CompositeKeyObjectBinding;
import com.savoirtech.hecate.pojo.binding.key.simple.SimpleKeyBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.type.GenericType;
import com.savoirtech.hecate.pojo.util.FunctionCacheLoader;

public class DefaultPojoBindingFactory implements PojoBindingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetProvider facetProvider;
    private final NamingStrategy namingStrategy;
    private final ConverterRegistry converterRegistry;
    private final LoadingCache<Class<?>, PojoBinding<?>> cache = CacheBuilder.newBuilder().build(new FunctionCacheLoader<>(this::createPojoBindingInternal));

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoBindingFactory(FacetProvider facetProvider, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        this.facetProvider = facetProvider;
        this.namingStrategy = namingStrategy;
        this.converterRegistry = converterRegistry;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoBindingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public <P> PojoBinding<P> createPojoBinding(Class<P> pojoType) {
        return (PojoBinding<P>) cache.getUnchecked(pojoType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private <P> PojoBinding<P> createPojoBindingInternal(Class<P> pojoType) {
        DefaultPojoBinding<P> binding = new DefaultPojoBinding<>(pojoType);
        List<Facet> keyFacets = injectFacetBindings(binding, pojoType);
        injectKeyBinding(binding, pojoType, keyFacets);
        return binding;
    }

    private <P> List<Facet> injectFacetBindings(DefaultPojoBinding<P> binding, Class<P> pojoType) {
        List<Facet> keyFacets = new LinkedList<>();
        facetProvider.getFacets(pojoType).stream().forEach(facet -> {
            if (facet.hasAnnotation(EmbeddedKey.class) ||
                    facet.hasAnnotation(PartitionKey.class) ||
                    facet.hasAnnotation(ClusteringColumn.class)) {
                keyFacets.add(facet);
            } else {
                GenericType facetType = facet.getType();
                Converter converter = converterRegistry.getConverter(facetType);
                String columnName = namingStrategy.getColumnName(facet);
                if (converter != null) {
                    binding.addFacetBinding(new SimpleFacetBinding(facet, columnName, converter));
                } else if (facetType.isList()) {
                    binding.addFacetBinding(new ListFacetBinding(facet, columnName, createElementBinding(facet, facetType.getElementType())));
                } else if (facetType.isSet()) {
                    binding.addFacetBinding(new SetFacetBinding(facet, columnName, createElementBinding(facet, facetType.getElementType())));
                } else if (facetType.isMap()) {
                    Converter keyConverter = converterRegistry.getConverter(facetType.getMapKeyType());
                    if (keyConverter == null) {
                        throw new HecateException("Invalid facet \"%s\"; no converter registered for key type \"%s\".", facet.getName(), facetType.getMapKeyType().getRawType().getCanonicalName());
                    }
                    binding.addFacetBinding(new MapFacetBinding(facet, columnName, keyConverter, createElementBinding(facet, facetType.getMapValueType())));
                } else if (facetType.isArray()) {
                    binding.addFacetBinding(new ArrayFacetBinding<>(facet, columnName, createElementBinding(facet, facetType.getArrayElementType())));
                } else if(facet.hasAnnotation(Embedded.class)) {
                    binding.addFacetBinding(new EmbeddedFacetBinding(facet, converterRegistry, namingStrategy));
                } else {
                    PojoBinding<?> refBinding = createPojoBinding(facetType.getRawType());
                    binding.addFacetBinding(refBinding.getKeyBinding().createReferenceBinding(facet, refBinding, namingStrategy));
                }
            }
        });
        return keyFacets;
    }

    private ElementBinding createElementBinding(Facet facet, GenericType elementType) {
        Converter converter = converterRegistry.getConverter(elementType);
        return converter != null ? new ScalarElementBinding(converter) : new PojoElementBinding(createPojoBinding(elementType.getRawType()), namingStrategy.getReferenceTableName(facet));
    }

    private <P> void injectKeyBinding(DefaultPojoBinding<P> binding, Class<P> pojoType, List<Facet> keyFacets) {
        switch (keyFacets.size()) {
            case 0:
                throw new HecateException("No key facets found for POJO type \"%s\".", pojoType.getCanonicalName());
            case 1:
                Facet keyFacet = keyFacets.get(0);
                if (keyFacet.hasAnnotation(EmbeddedKey.class)) {
                    binding.setKeyBinding(createCompositeKeyObjectBinding(keyFacet));
                } else if (keyFacet.hasAnnotation(PartitionKey.class)) {
                    binding.setKeyBinding(createSimpleKeyBinding(keyFacet));
                } else {
                    throw new HecateException("No @PartitionKey facets found for POJO type \"%s\".", pojoType.getCanonicalName());
                }
                break;
            default:
                binding.setKeyBinding(createCompositeKeyBinding(keyFacets));
        }
    }

    private KeyBinding createCompositeKeyObjectBinding(Facet facet) {
        return new CompositeKeyObjectBinding(facet, facetProvider, converterRegistry, namingStrategy);
    }

    private KeyBinding createSimpleKeyBinding(Facet keyFacet) {
        Converter converter = converterRegistry.getConverter(keyFacet.getType());
        if(converter == null) {
            throw new HecateException("No converter found for @PartitionKey facet \"%s\".", keyFacet.getName());
        }
        return new SimpleKeyBinding(keyFacet, namingStrategy.getColumnName(keyFacet), converter);
    }

    private KeyBinding createCompositeKeyBinding(List<Facet> keyFacets) {
        return new CompositeKeyBinding(keyFacets, namingStrategy, converterRegistry);
    }
}
