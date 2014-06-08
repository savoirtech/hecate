package com.savoirtech.hecate.cql3.convert.map;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterFactory;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.util.GenericType;

public class MapConverterFactory implements ValueConverterFactory {
//----------------------------------------------------------------------------------------------------------------------
// ValueConverterFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Class<? extends ValueConverter> converterType() {
        return MapConverter.class;
    }

    @Override
    public ValueConverter createConverter(GenericType type, ValueConverterRegistry registry) {
        return new MapConverter(registry.getValueConverter(type.getMapKeyType()), registry.getValueConverter(type.getMapValueType()));
    }
}
