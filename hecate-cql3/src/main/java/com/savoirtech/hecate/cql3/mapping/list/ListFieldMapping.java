package com.savoirtech.hecate.cql3.mapping.list;

import com.savoirtech.hecate.cql3.mapping.ListBasedMapping;
import com.savoirtech.hecate.cql3.type.ColumnType;

import java.lang.reflect.Field;
import java.util.List;

public class ListFieldMapping extends ListBasedMapping {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ListFieldMapping(Field field, ColumnType<Object> columnType) {
        super(field, columnType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object fromCassandraList(List<Object> cassandraList) {
        return mapFromCassandraValues(cassandraList);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<Object> toCassandraList(Object fieldValue) {
        return mapToCassandraValues((List<Object>) fieldValue);
    }
}
