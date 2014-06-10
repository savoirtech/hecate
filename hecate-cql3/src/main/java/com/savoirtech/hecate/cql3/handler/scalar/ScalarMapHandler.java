package com.savoirtech.hecate.cql3.handler.scalar;

import com.savoirtech.hecate.cql3.convert.ValueConverter;

public class ScalarMapHandler extends com.savoirtech.hecate.cql3.handler.AbstractMapHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final ValueConverter keyConverter;
    private final ValueConverter valueConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public ScalarMapHandler(ValueConverter keyConverter, ValueConverter valueConverter) {
        super(keyConverter.getDataType(), valueConverter.getDataType());
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object toCassandraKey(Object key) {
        return keyConverter.toCassandraValue(key);
    }

    @Override
    protected Object toCassandraValue(Object value) {
        return valueConverter.toCassandraValue(value);
    }

    @Override
    protected Object toFacetKey(Object key) {
        return keyConverter.fromCassandraValue(key);
    }

    @Override
    protected Object toFacetValue(Object value) {
        return valueConverter.fromCassandraValue(value);
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return null;
    }
}
