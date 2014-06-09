package com.savoirtech.hecate.cql3.convert.list;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;

import java.util.ArrayList;
import java.util.List;

public class ListConverter implements ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter elementConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListConverter(ValueConverter elementConverter) {
        this.elementConverter = elementConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object fromCassandraValue(Object value, Hydrator hydrator) {
        if (value == null) {
            return null;
        }
        List<Object> cassandraList = (List<Object>) value;
        List<Object> modelList = new ArrayList<>(cassandraList.size());
        for (Object cassandraValue : cassandraList) {
            modelList.add(elementConverter.fromCassandraValue(cassandraValue, hydrator));
        }
        return modelList;
    }

    @Override
    public DataType getDataType() {
        return DataType.list(elementConverter.getDataType());
    }

    @Override
    public Object toCassandraValue(Object value, Dehydrator dehydrator) {
        if (value == null) {
            return null;
        }
        List<Object> modelList = (List<Object>) value;
        List<Object> cassandraList = new ArrayList<>(modelList.size());
        for (Object modelValue : modelList) {
            cassandraList.add(elementConverter.toCassandraValue(modelValue, dehydrator));
        }
        return cassandraList;
    }
}
