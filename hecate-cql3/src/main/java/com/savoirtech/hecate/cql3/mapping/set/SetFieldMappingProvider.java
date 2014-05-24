package com.savoirtech.hecate.cql3.mapping.set;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;

import java.lang.reflect.Field;
import java.util.Set;

public class SetFieldMappingProvider implements FieldMappingProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnTypeRegistry registry;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetFieldMappingProvider(ColumnTypeRegistry registry) {
        this.registry = registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public FieldMapping createFieldMapping(Field field) {
        return new SetFieldMapping(field, (ColumnType<Object>) registry.getColumnType(setElementType(field)));
    }

    @Override
    public boolean supports(Field field) {
        final Class<?> elementType = setElementType(field);
        return elementType != null && registry.getColumnType(elementType) != null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Class<?> setElementType(Field field) {
        if (Set.class.isAssignableFrom(field.getType())) {
            return ReflectionUtils.setElementType(field.getGenericType());
        }
        return null;
    }
}
