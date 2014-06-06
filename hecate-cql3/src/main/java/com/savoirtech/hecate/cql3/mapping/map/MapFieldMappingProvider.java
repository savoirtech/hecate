package com.savoirtech.hecate.cql3.mapping.map;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;

import java.lang.reflect.Field;
import java.util.Map;

public class MapFieldMappingProvider implements FieldMappingProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnTypeRegistry registry;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapFieldMappingProvider(ColumnTypeRegistry registry) {
        this.registry = registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public FieldMapping createFieldMapping(Field field, PojoDaoFactory factory) {
        return new MapFieldMapping(field,
                (ColumnType<Object>) registry.getColumnType(keyType(field)),
                (ColumnType<Object>) registry.getColumnType(valueType(field)));
    }

    @Override
    public boolean supports(Field field) {
        Class<?> keyType = keyType(field);
        Class<?> valueType = valueType(field);
        return keyType != null && registry.getColumnType(keyType) != null && valueType != null && registry.getColumnType(valueType) != null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Class<?> keyType(Field field) {
        if (Map.class.isAssignableFrom(field.getType())) {
            return ReflectionUtils.mapKeyType(field.getGenericType());
        }
        return null;
    }

    private Class<?> valueType(Field field) {
        if (Map.class.isAssignableFrom(field.getType())) {
            return ReflectionUtils.mapValueType(field.getGenericType());
        }
        return null;
    }
}
