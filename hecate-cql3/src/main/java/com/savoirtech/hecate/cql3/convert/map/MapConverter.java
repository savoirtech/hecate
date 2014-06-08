package com.savoirtech.hecate.cql3.convert.map;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;

import java.util.HashMap;
import java.util.Map;

public class MapConverter implements ValueConverter {

    private final ValueConverter keyConverter;
    private final ValueConverter valueConverter;

    public MapConverter(ValueConverter keyConverter, ValueConverter valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

    @Override
    public Object fromCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        final Map<Object, Object> cassandraMap = (Map<Object, Object>) value;
        final Map<Object, Object> modelMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : cassandraMap.entrySet()) {
            modelMap.put(keyConverter.fromCassandraValue(entry.getKey()), valueConverter.fromCassandraValue(entry.getValue()));
        }
        return modelMap;

    }

    @Override
    public DataType getDataType() {
        return DataType.map(keyConverter.getDataType(), valueConverter.getDataType());
    }

    @Override
    public Object toCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        final Map<Object, Object> modelMap = (Map<Object, Object>) value;
        final Map<Object, Object> cassandraMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : modelMap.entrySet()) {
            cassandraMap.put(keyConverter.toCassandraValue(entry.getKey()), valueConverter.toCassandraValue(entry.getValue()));
        }
        return cassandraMap;
    }
}
