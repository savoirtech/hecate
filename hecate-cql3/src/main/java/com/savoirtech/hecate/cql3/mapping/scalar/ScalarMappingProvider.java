package com.savoirtech.hecate.cql3.mapping.scalar;

import com.savoirtech.hecate.cql3.dao.PojoDaoFactory;
import com.savoirtech.hecate.cql3.mapping.FieldMapping;
import com.savoirtech.hecate.cql3.mapping.FieldMappingProvider;
import com.savoirtech.hecate.cql3.type.ColumnType;
import com.savoirtech.hecate.cql3.type.ColumnTypeRegistry;

import java.lang.reflect.Field;

public class ScalarMappingProvider implements FieldMappingProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnTypeRegistry registry;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarMappingProvider(ColumnTypeRegistry registry) {
        this.registry = registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMappingProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public FieldMapping createFieldMapping(Field field, PojoDaoFactory factory) {
        ColumnType<Object> columnType = (ColumnType<Object>) registry.getColumnType(field.getType());
        return new ScalarMapping(field, columnType);
    }

    @Override
    public boolean supports(Field field) {
        return registry.getColumnType(field.getType()) != null;
    }


}
