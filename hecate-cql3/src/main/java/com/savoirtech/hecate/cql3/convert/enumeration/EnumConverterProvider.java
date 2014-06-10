package com.savoirtech.hecate.cql3.convert.enumeration;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterProvider;

public class EnumConverterProvider implements ValueConverterProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueConverterProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<? extends ValueConverter> converterType() {
        return EnumConverter.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ValueConverter createConverter(Class<?> valueType) {
        return new EnumConverter((Class<? extends Enum>) valueType);
    }
}
