package com.savoirtech.hecate.cql3.meta.def;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.def.DefaultValueConverterRegistry;
import com.savoirtech.hecate.cql3.mapping.ValueMapping;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptorFactory;
import com.savoirtech.hecate.cql3.value.Value;
import com.savoirtech.hecate.cql3.value.ValueProvider;
import com.savoirtech.hecate.cql3.value.field.FieldValueProvider;

public class DefaultPojoDescriptorFactory implements PojoDescriptorFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private ValueConverterRegistry valueConverterRegistry = DefaultValueConverterRegistry.defaultRegistry();
    private ValueProvider valueProvider = new FieldValueProvider();

//----------------------------------------------------------------------------------------------------------------------
// PojoDescriptorFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> PojoDescriptor<P> getPojoDescriptor(Class<P> pojoType) {
        PojoDescriptor<P> descriptor = new PojoDescriptor<>(pojoType);
        for (Value value : valueProvider.getValues(pojoType)) {
            ValueConverter converter = valueConverterRegistry.getValueConverter(value.getType());
            descriptor.addMapping(new ValueMapping(value, converter));
        }
        return descriptor;
    }
}
