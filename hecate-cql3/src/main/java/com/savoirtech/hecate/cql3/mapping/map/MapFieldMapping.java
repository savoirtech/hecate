package com.savoirtech.hecate.cql3.mapping.map;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.savoirtech.hecate.cql3.mapping.AbstractFieldMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class MapFieldMapping extends AbstractFieldMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ColumnType<Object> keyType;
    private final ColumnType<Object> valueType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public MapFieldMapping(Field field, ColumnType<Object> keyType, ColumnType<Object> valueType) {
        super(field);
        this.keyType = keyType;
        this.valueType = valueType;
    }

//----------------------------------------------------------------------------------------------------------------------
// FieldMapping Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType columnType() {
        return DataType.map(keyType.getDataType(), valueType.getDataType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void populateFromRow(Object root, Row row, int columnIndex) {

        Map<Object, Object> cassandraValues = (Map<Object, Object>) row.getMap(columnIndex, keyType.getDataType().asJavaClass(), valueType.getDataType().asJavaClass());
        if (cassandraValues == null) {
            setFieldValue(root, null);
        } else {
            Map<Object, Object> values = new HashMap<>();
            for (Map.Entry<Object, Object> entry : cassandraValues.entrySet()) {
                values.put(keyType.fromCassandraValue(entry.getKey()), valueType.fromCassandraValue(entry.getValue()));
            }
            setFieldValue(root, values);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object rawCassandraValue(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        Map<Object, Object> values = (Map<Object, Object>) rawValue;
        Map<Object, Object> cassandraValues = new HashMap<>();
        for (Map.Entry<Object, Object> entry : values.entrySet()) {
            cassandraValues.put(keyType.toCassandraValue(entry.getKey()), valueType.toCassandraValue(entry.getValue()));
        }
        return cassandraValues;
    }
}
