/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.convert.def;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterProvider;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.NativeConverter;
import com.savoirtech.hecate.pojo.convert.binary.ByteArrayConverter;
import com.savoirtech.hecate.pojo.convert.enumeration.EnumConverterProvider;
import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class DefaultConverterRegistry implements ConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConverterRegistry.class);

    private final Map<Class<?>, ConverterProvider> providers;
    private final Set<Class<?>> unsupportedTypes = new CopyOnWriteArraySet<>();

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static ConverterRegistry defaultRegistry() {
        final DefaultConverterRegistry registry = new DefaultConverterRegistry();
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
        registry.registerConverter(ByteBuffer.class, NativeConverter.BLOB);
        registry.registerConverter(byte[].class, new ByteArrayConverter());
        return registry;
    }

    @SuppressWarnings("unchecked")
    public static List<Class<?>> getSupertypes(Class<?> type) {
        List<Class<?>> supertypes = new LinkedList<>();
        supertypes.add(type.getSuperclass());
        Collections.addAll(supertypes, type.getInterfaces());
        return supertypes;
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultConverterRegistry() {
        providers = new MapMaker().makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// ConverterRegistry Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Converter getConverter(Class<?> valueType) {
        ConverterProvider provider = findProvider(valueType);
        if (provider == null) {
            return null;
        }
        return provider.createConverter(valueType);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private ConverterProvider findProvider(Class<?> valueType) {
        if (valueType == null) {
            return null;
        }

        if (unsupportedTypes.contains(valueType)) {
            return null;
        }

        ConverterProvider provider = providers.get(valueType);
        if (provider != null) {
            return provider;
        }

        List<Class<?>> supertypes = getSupertypes(valueType);
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

    public void registerConverter(Class<?> valueType, Converter converter) {
        registerConverter(valueType, new ConstantProvider(converter));
    }

    public void registerConverter(Class<?> valueType, ConverterProvider factory) {
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

    private static final class ConstantProvider implements ConverterProvider {
        private final Converter converter;

        private ConstantProvider(Converter converter) {
            this.converter = converter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Converter createConverter(Class<?> valueType) {
            return converter;
        }

        @Override
        public Class<? extends Converter> converterType() {
            return converter.getClass();
        }
    }
}
