package com.savoirtech.hecate.cql3.convert.set;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterFactory;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.util.GenericType;

public class SetConverterFactory implements ValueConverterFactory {

    @Override
    public ValueConverter createConverter(GenericType type, ValueConverterRegistry registry) {
        return new SetConverter(registry.getValueConverter(type.getSetElementType()));
    }

    @Override
    public Class<? extends ValueConverter> converterType() {
        return SetConverter.class;
    }
}
