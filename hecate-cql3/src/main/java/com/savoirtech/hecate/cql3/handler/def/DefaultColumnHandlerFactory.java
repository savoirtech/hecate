package com.savoirtech.hecate.cql3.handler.def;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.def.DefaultValueConverterRegistry;
import com.savoirtech.hecate.cql3.exception.HecateException;
import com.savoirtech.hecate.cql3.handler.ColumnHandler;
import com.savoirtech.hecate.cql3.handler.ColumnHandlerFactory;
import com.savoirtech.hecate.cql3.handler.pojo.PojoArrayHandler;
import com.savoirtech.hecate.cql3.handler.pojo.PojoListHandler;
import com.savoirtech.hecate.cql3.handler.pojo.PojoMapHandler;
import com.savoirtech.hecate.cql3.handler.pojo.PojoSetHandler;
import com.savoirtech.hecate.cql3.handler.pojo.PojoValueHandler;
import com.savoirtech.hecate.cql3.handler.scalar.ScalarArrayHandler;
import com.savoirtech.hecate.cql3.handler.scalar.ScalarListHandler;
import com.savoirtech.hecate.cql3.handler.scalar.ScalarMapHandler;
import com.savoirtech.hecate.cql3.handler.scalar.ScalarSetHandler;
import com.savoirtech.hecate.cql3.handler.scalar.ScalarValueHandler;
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

    private ValueConverterRegistry registry = DefaultValueConverterRegistry.defaultRegistry();
    private PojoMetadataFactory pojoMetadataFactory = new DefaultPojoMetadataFactory();

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandlerFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public ColumnHandler getColumnHandler(FacetMetadata facetMetadata) {
        final GenericType facetType = facetMetadata.getFacet().getType();
        ValueConverter converter = registry.getValueConverter(facetType.getRawType());
        if (converter != null) {
            return new ScalarValueHandler(converter);
        }
        if (List.class.equals(facetType.getRawType())) {
            return createListHandler(facetMetadata, facetType.getListElementType());
        }
        if (Set.class.equals(facetType.getRawType())) {
            return createSetHandler(facetMetadata, facetType.getSetElementType());
        }
        if (Map.class.equals(facetType.getRawType())) {
            return createMapHandler(facetMetadata, facetType.getMapKeyType(), facetType.getMapValueType());
        }
        if (facetType.getRawType().isArray()) {
            return createArrayHandler(facetMetadata, facetType.getArrayElementType());
        }
        PojoMetadata metadata = pojoMetadataFactory.getPojoMetadata(facetType.getRawType());
        return new PojoValueHandler(facetMetadata, metadata, getIdentifierConverter(metadata));
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setPojoMetadataFactory(PojoMetadataFactory pojoMetadataFactory) {
        this.pojoMetadataFactory = pojoMetadataFactory;
    }

    public void setRegistry(ValueConverterRegistry registry) {
        this.registry = registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private ColumnHandler createArrayHandler(FacetMetadata facetMetadata, GenericType elementType) {
        ValueConverter converter = registry.getValueConverter(elementType.getRawType());
        if (converter != null) {
            return new ScalarArrayHandler(elementType.getRawType(), converter);
        }
        final PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(elementType.getRawType());
        return new PojoArrayHandler(facetMetadata, pojoMetadata, getIdentifierConverter(pojoMetadata));
    }

    private ValueConverter getIdentifierConverter(PojoMetadata pojoMetadata) {
        final ValueConverter converter = registry.getValueConverter(pojoMetadata.getIdentifierFacet().getFacet().getType().getRawType());
        if (converter == null) {
            throw new HecateException(String.format("Unable to cascade POJO type %s (non-scalar identifier).", pojoMetadata.getPojoType().getCanonicalName()));
        }
        return converter;
    }

    private ColumnHandler createListHandler(FacetMetadata facetMetadata, GenericType elementType) {
        ValueConverter converter = registry.getValueConverter(elementType.getRawType());
        if (converter != null) {
            return new ScalarListHandler(converter);
        }
        final PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(elementType.getRawType());
        return new PojoListHandler(facetMetadata, pojoMetadata, getIdentifierConverter(pojoMetadata));
    }


    private ColumnHandler createMapHandler(FacetMetadata facetMetadata, GenericType keyType, GenericType valueType) {
        ValueConverter keyConverter = Validate.notNull(registry.getValueConverter(keyType.getRawType()), "Invalid map key type %s (must be scalar).", keyType.getRawType().toString());
        ValueConverter valueConverter = registry.getValueConverter(valueType.getRawType());
        if (valueConverter != null) {
            return new ScalarMapHandler(keyConverter, valueConverter);
        }
        final PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(valueType.getRawType());
        return new PojoMapHandler(keyConverter, facetMetadata, pojoMetadata, getIdentifierConverter(pojoMetadata));
    }

    private ColumnHandler createSetHandler(FacetMetadata facetMetadata, GenericType elementType) {
        ValueConverter converter = registry.getValueConverter(elementType.getRawType());
        if (converter != null) {
            return new ScalarSetHandler(converter);
        }
        final PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(elementType.getRawType());
        return new PojoSetHandler(facetMetadata, pojoMetadata, getIdentifierConverter(pojoMetadata));
    }
}
