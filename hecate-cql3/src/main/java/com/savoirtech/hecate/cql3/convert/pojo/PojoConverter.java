package com.savoirtech.hecate.cql3.convert.pojo;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.persistence.Dehydrator;
import com.savoirtech.hecate.cql3.persistence.Hydrator;

public class PojoConverter implements ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<?> pojoType;
    private final ValueConverter identifierConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoConverter(Class<?> pojoType, ValueConverter identifierConverter) {
        this.pojoType = pojoType;
        this.identifierConverter = identifierConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object fromCassandraValue(Object value, Hydrator hydrator) {
        return hydrator.newPojo(pojoType, identifierConverter.fromCassandraValue(value, hydrator));
    }

    @Override
    public DataType getDataType() {
        return identifierConverter.getDataType();
    }

    @Override
    public Object toCassandraValue(Object value, Dehydrator dehydrator) {
        return identifierConverter.toCassandraValue(dehydrator.getIdentifier(pojoType, value), dehydrator);
    }
}
