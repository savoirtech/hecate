package com.savoirtech.hecate.cql3.mapping.set;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.mapping.AbstractFieldMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

public class SetFieldMapping extends AbstractFieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnType<Object> elementType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SetFieldMapping(Field field, ColumnType<Object> elementType) {
        super(field);
        this.elementType = elementType;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType columnType() {
        return DataType.set(elementType.getDataType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void populateFromRow(Object root, Row row, int columnIndex) {
        final Set<Object> cassandraValues = (Set<Object>) row.getSet(columnIndex, elementType.getDataType().asJavaClass());
        if (cassandraValues == null) {
            setFieldValue(root, null);
        } else {
            setFieldValue(root, fromCassandraValues(cassandraValues));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object rawCassandraValue(Object rawValue) {
        if (rawValue == null) {
            return null;
        } else {
            final Set<Object> values = (Set<Object>) rawValue;
            final Set<Object> cassandraValues = new HashSet<>();
            for (Object value : values) {
                cassandraValues.add(elementType.toCassandraValue(value));
            }
            return cassandraValues;
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Set<Object> fromCassandraValues(Set<Object> cassandraValues) {
        final Set<Object> values = new HashSet<>();
        for (Object cassandraValue : cassandraValues) {
            values.add(elementType.fromCassandraValue(cassandraValue));
        }
        return values;
    }
}
