/*
 * Copyright (c) 2014. Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.convert.def;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.convert.NativeConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterProvider;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.enumeration.EnumConverterProvider;
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
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultValueConverterRegistry implements ValueConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultValueConverterRegistry.class);

    private final Map<Class<?>, ValueConverterProvider> providers;
    private final Set<Class<?>> unsupportedTypes = new CopyOnWriteArraySet<>();

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
        registry.registerConverter(Enum.class, new EnumConverterProvider());
        return registry;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultValueConverterRegistry() {
        providers = new MapMaker().makeMap();
    }

    public DefaultValueConverterRegistry(int concurrencyLevel) {
        providers = new MapMaker().concurrencyLevel(concurrencyLevel).makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// ValueConverterRegistry Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public ValueConverter getValueConverter(Class<?> valueType) {
        ValueConverterProvider provider = findProvider(valueType);
        if (provider == null) {
            return null;
        }
        return provider.createConverter(valueType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private ValueConverterProvider findProvider(Class<?> valueType) {
        if (valueType == null) {
            return null;
        }

        if (unsupportedTypes.contains(valueType)) {
            return null;
        }

        ValueConverterProvider provider = providers.get(valueType);
        if (provider != null) {
            return provider;
        }

        List<Class<?>> supertypes = ReflectionUtils.getSupertypes(valueType);
        for (Class<?> supertype : supertypes) {
            provider = findProvider(supertype);
            if (provider != null) {
                LOGGER.debug("Adding shortcut mapping {} -> {}", valueType.getCanonicalName(), provider.converterType().getCanonicalName());
                providers.put(valueType, provider);
                return provider;
            }
        }
        unsupportedTypes.add(valueType);
        return null;
    }

    public void registerConverter(Class<?> valueType, ValueConverter converter) {
        registerConverter(valueType, new ConstantProvider(converter));
    }

    public void registerConverter(Class<?> valueType, ValueConverterProvider factory) {
        LOGGER.debug("Adding factory {} -> {}...", valueType.getCanonicalName(), factory.converterType().getCanonicalName());
        providers.put(valueType, factory);
        if (ClassUtils.isPrimitiveWrapper(valueType)) {
            registerConverter(ClassUtils.wrapperToPrimitive(valueType), factory);
        }
        providers.put(valueType, factory);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static final class ConstantProvider implements ValueConverterProvider {
        private final ValueConverter valueConverter;

        private ConstantProvider(ValueConverter valueConverter) {
            this.valueConverter = valueConverter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public ValueConverter createConverter(Class<?> valueType) {
            return valueConverter;
        }

        @Override
        public Class<? extends ValueConverter> converterType() {
            return valueConverter.getClass();
        }
    }
}
