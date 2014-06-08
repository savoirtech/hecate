package com.savoirtech.hecate.cql3.convert;

import com.savoirtech.hecate.cql3.util.GenericType;

public interface ValueConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    ValueConverter getValueConverter(GenericType value);
}
