package com.savoirtech.hecate.cql3.mapping.list;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;

import java.lang.reflect.Field;
import java.util.List;

public class ListFieldMappingProvider implements FieldMappingProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnTypeRegistry registry;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListFieldMappingProvider(ColumnTypeRegistry registry) {
        this.registry = registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingProvider Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    @SuppressWarnings("unchecked")
    public FieldMapping createFieldMapping(Field field) {
        return new ListFieldMapping(field, (ColumnType<Object>) registry.getColumnType(listElementType(field)));
    }

    @Override
    public boolean supports(Field field) {
        final Class<?> elementType = listElementType(field);
        return elementType != null && registry.getColumnType(elementType) != null;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Class<?> listElementType(Field field) {
        if (List.class.isAssignableFrom(field.getType())) {
            return ReflectionUtils.listElementType(field.getGenericType());
        }
        return null;
    }
}
