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

package com.savoirtech.hecate.cql3.handler.def;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.def.DefaultValueConverterRegistry;
import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.handler.ArrayHandler;
import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.handler.ColumnHandlerFactory;
import com.savoirtech.hecate.cql3.handler.ListHandler;
import com.savoirtech.hecate.cql3.handler.MapHandler;
import com.savoirtech.hecate.cql3.handler.SetHandler;
import com.savoirtech.hecate.cql3.handler.SimpleHandler;
import com.savoirtech.hecate.cql3.handler.delegate.ColumnHandlerDelegate;
import com.savoirtech.hecate.cql3.handler.delegate.PojoDelegate;
import com.savoirtech.hecate.cql3.handler.delegate.ScalarDelegate;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.meta.def.DefaultPojoMetadataFactory;
import com.savoirtech.hecate.cql3.util.GenericType;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultColumnHandlerFactory implements ColumnHandlerFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverterRegistry registry;
    private final PojoMetadataFactory pojoMetadataFactory;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static Class<?> elementType(GenericType genericType) {
        if (List.class.equals(genericType.getRawType())) {
            return genericType.getListElementType().getRawType();
        }
        if (Set.class.equals(genericType.getRawType())) {
            return genericType.getSetElementType().getRawType();
        }
        if (Map.class.equals(genericType.getRawType())) {
            return genericType.getMapValueType().getRawType();
        }
        if (genericType.getRawType().isArray()) {
            return genericType.getArrayElementType().getRawType();
        }
        return genericType.getRawType();
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultColumnHandlerFactory() {
        this(DefaultValueConverterRegistry.defaultRegistry(), new DefaultPojoMetadataFactory());
    }

    public DefaultColumnHandlerFactory(PojoMetadataFactory pojoMetadataFactory) {
        this(DefaultValueConverterRegistry.defaultRegistry(), pojoMetadataFactory);
    }

    public DefaultColumnHandlerFactory(ValueConverterRegistry registry, PojoMetadataFactory pojoMetadataFactory) {
        this.registry = registry;
        this.pojoMetadataFactory = pojoMetadataFactory;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandlerFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public ColumnHandler<Object, Object> getColumnHandler(FacetMetadata facetMetadata) {
        final GenericType facetType = facetMetadata.getFacet().getType();
        final Class<?> elementType = elementType(facetType);
        ValueConverter converter = registry.getValueConverter(elementType);
        if (converter != null) {
            return createColumnHandler(facetType, new ScalarDelegate(converter));
        } else {
            final Class<?> pojoType = elementType(facetType);
            final PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(pojoType);
            return createColumnHandler(facetType, new PojoDelegate(pojoMetadata, facetMetadata, getIdentifierConverter(pojoMetadata)));
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private ColumnHandler createColumnHandler(GenericType facetType, ColumnHandlerDelegate delegate) {
        if (List.class.equals(facetType.getRawType())) {
            return new ListHandler(delegate);
        }
        if (Set.class.equals(facetType.getRawType())) {
            return new SetHandler(delegate);
        }
        if (Map.class.equals(facetType.getRawType())) {
            final Class<?> keyType = facetType.getMapKeyType().getRawType();
            final ValueConverter keyConverter = Validate.notNull(registry.getValueConverter(keyType), "Invalid map key type %s (must be scalar).", keyType.getCanonicalName());
            return new MapHandler(keyConverter, delegate);
        }
        if (facetType.getRawType().isArray()) {
            return new ArrayHandler(facetType.getRawType().getComponentType(), delegate);
        }
        return new SimpleHandler(delegate);
    }

    private ValueConverter getIdentifierConverter(PojoMetadata pojoMetadata) {
        final ValueConverter converter = registry.getValueConverter(pojoMetadata.getIdentifierFacet().getFacet().getType().getRawType());
        if (converter == null) {
            throw new HecateException(String.format("Unable to cascade POJO type %s (non-scalar identifier).", pojoMetadata.getPojoType().getCanonicalName()));
        }
        return converter;
    }
}
