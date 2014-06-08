package com.savoirtech.hecate.cql3.convert;

import com.savoirtech.hecate.cql3.util.GenericType;

public interface ValueConverterFactory {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Class<? extends ValueConverter> converterType();

    ValueConverter createConverter(GenericType type, ValueConverterRegistry registry);
}
