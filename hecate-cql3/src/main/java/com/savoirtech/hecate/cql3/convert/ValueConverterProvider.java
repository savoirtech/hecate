package com.savoirtech.hecate.cql3.convert;

public interface ValueConverterProvider {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Class<? extends ValueConverter> converterType();

    ValueConverter createConverter(Class<?> valueType);
}
