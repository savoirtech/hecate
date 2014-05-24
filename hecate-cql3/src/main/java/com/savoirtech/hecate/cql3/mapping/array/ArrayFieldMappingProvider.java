package com.savoirtech.hecate.cql3.mapping.array;

import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;

import java.lang.reflect.Field;

public class ArrayFieldMappingProvider implements FieldMappingProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnTypeRegistry columnTypeRegistry;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ArrayFieldMappingProvider(ColumnTypeRegistry columnTypeRegistry) {
        this.columnTypeRegistry = columnTypeRegistry;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public FieldMapping createFieldMapping(Field field, PojoDaoFactory factory) {
        return new ArrayFieldMapping(field, (ColumnType<Object>) columnTypeRegistry.getColumnType(field.getType().getComponentType()));
    }

    @Override
    public boolean supports(Field field) {
        return field.getType().isArray() && columnTypeRegistry.getColumnType(field.getType().getComponentType()) != null;
    }
}
