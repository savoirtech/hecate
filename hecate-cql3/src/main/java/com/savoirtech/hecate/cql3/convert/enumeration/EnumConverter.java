package com.savoirtech.hecate.cql3.convert.enumeration;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;

public class EnumConverter implements ValueConverter {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<? extends Enum> enumType;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public EnumConverter(Class<? extends Enum> enumType) {
        this.enumType = enumType;
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Object fromCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        return Enum.valueOf(enumType, value.toString());
    }

    @Override
    public DataType getDataType() {
        return DataType.varchar();
    }

    @Override
    public Object toCassandraValue(Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }
}
