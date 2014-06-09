package com.savoirtech.hecate.cql3.convert.map;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;

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
    @SuppressWarnings("unchecked")
    public Object fromCassandraValue(Object value, Hydrator hydrator) {
        if (value == null) {
            return null;
        }
        final Map<Object, Object> cassandraMap = (Map<Object, Object>) value;
        final Map<Object, Object> modelMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : cassandraMap.entrySet()) {
            modelMap.put(keyConverter.fromCassandraValue(entry.getKey(), hydrator), valueConverter.fromCassandraValue(entry.getValue(), hydrator));
        }
        return modelMap;

    }

    @Override
    public DataType getDataType() {
        return DataType.map(keyConverter.getDataType(), valueConverter.getDataType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object toCassandraValue(Object value, Dehydrator dehydrator) {
        if (value == null) {
            return null;
        }
        final Map<Object, Object> modelMap = (Map<Object, Object>) value;
        final Map<Object, Object> cassandraMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : modelMap.entrySet()) {
            cassandraMap.put(
                    keyConverter.toCassandraValue(entry.getKey(), dehydrator),
                    valueConverter.toCassandraValue(entry.getValue(), dehydrator));
        }
        return cassandraMap;
    }
}
