package com.savoirtech.hecate.cql3.convert.def;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.convert.NativeConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterFactory;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.array.ArrayConverter;
import com.savoirtech.hecate.cql3.convert.list.ListConverterFactory;
import com.savoirtech.hecate.cql3.convert.map.MapConverterFactory;
import com.savoirtech.hecate.cql3.convert.set.SetConverterFactory;
import com.savoirtech.hecate.cql3.exception.ValueConverterNotFoundException;
import com.savoirtech.hecate.cql3.util.GenericType;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DefaultValueConverterRegistry implements ValueConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final static Logger LOGGER = LoggerFactory.getLogger(DefaultValueConverterRegistry.class);

    private final Map<Class<?>, ValueConverterFactory> factories;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static ValueConverterRegistry defaultRegistry() {
        final DefaultValueConverterRegistry registry = new DefaultValueConverterRegistry();
        registry.registerConverter(Boolean.class, NativeConverter.BOOLEAN);
        registry.registerConverter(BigDecimal.class, NativeConverter.BIG_DECIMAL);
        registry.registerConverter(BigInteger.class, NativeConverter.BIG_INTEGER);
        registry.registerConverter(Date.class, NativeConverter.DATE);
        registry.registerConverter(Double.class, NativeConverter.DOUBLE);
        registry.registerConverter(Float.class, NativeConverter.FLOAT);
        registry.registerConverter(InetAddress.class, NativeConverter.INET);
        registry.registerConverter(Integer.class, NativeConverter.INTEGER);
        registry.registerConverter(Long.class, NativeConverter.LONG);
        registry.registerConverter(String.class, NativeConverter.STRING);
        registry.registerConverter(UUID.class, NativeConverter.UUID);
        registry.registerConverter(List.class, new ListConverterFactory());
        registry.registerConverter(Set.class, new SetConverterFactory());
        registry.registerConverter(Map.class, new MapConverterFactory());
        return registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultValueConverterRegistry() {
        factories = new MapMaker().makeMap();
    }

    public DefaultValueConverterRegistry(int concurrencyLevel) {
        factories = new MapMaker().concurrencyLevel(concurrencyLevel).makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverterRegistry Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public ValueConverter getValueConverter(GenericType type) {
        if (type.getRawType().isArray()) {
            return new ArrayConverter(type.getArrayElementType().getRawType(), getValueConverter(type.getArrayElementType()));
        }
        ValueConverterFactory factory = findFactory(type.getRawType());
        if (factory == null) {
            throw new ValueConverterNotFoundException(type.getRawType());
        }
        return factory.createConverter(type, this);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private ValueConverterFactory findFactory(Class<?> valueType) {
        if (valueType == null) {
            return null;
        }

        ValueConverterFactory factory = factories.get(valueType);
        if (factory != null) {
            return factory;
        }

        return null;
    }

    public void registerConverter(Class<?> valueType, ValueConverter converter) {
        registerConverter(valueType, new ConstantFactory(converter));
    }

    public void registerConverter(Class<?> valueType, ValueConverterFactory factory) {
        LOGGER.debug("Adding factory {} -> {}...", valueType.getCanonicalName(), factory.converterType().getCanonicalName());
        factories.put(valueType, factory);
        if (ClassUtils.isPrimitiveWrapper(valueType)) {
            registerConverter(ClassUtils.wrapperToPrimitive(valueType), factory);
        }
        factories.put(valueType, factory);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class ConstantFactory implements ValueConverterFactory {
        private final ValueConverter valueConverter;

        private ConstantFactory(ValueConverter valueConverter) {
            this.valueConverter = valueConverter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ValueConverter createConverter(GenericType value, ValueConverterRegistry registry) {
            return valueConverter;
        }

        @Override
        public Class<? extends ValueConverter> converterType() {
            return valueConverter.getClass();
        }
    }
}
