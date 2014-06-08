package com.savoirtech.hecate.cql3.convert.list;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterFactory;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.util.GenericType;

public class ListConverterFactory implements ValueConverterFactory {
//----------------------------------------------------------------------------------------------------------------------
// ValueConverterFactory Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Class<? extends ValueConverter> converterType() {
        return ListConverter.class;
    }

    @Override
    public ValueConverter createConverter(GenericType type, ValueConverterRegistry registry) {
        return new ListConverter(registry.getValueConverter(type.getListElementType()));
    }
}
