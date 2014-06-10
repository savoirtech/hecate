package com.savoirtech.hecate.cql3.convert;

public interface ValueConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    ValueConverter getValueConverter(Class<?> valueType);
}
