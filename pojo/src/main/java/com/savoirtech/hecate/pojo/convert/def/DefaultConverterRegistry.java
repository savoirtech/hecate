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

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterProvider;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.NativeConverter;
import com.savoirtech.hecate.pojo.convert.binary.ByteArrayConverter;
import com.savoirtech.hecate.pojo.convert.enumeration.EnumConverterProvider;
import com.savoirtech.hecate.pojo.convert.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultConverterRegistry implements ConverterRegistry {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConverterRegistry.class);

    private final Map<Class<?>, ConverterProvider> providers = new MapMaker().makeMap();
    private final Set<Class<?>> unsupportedTypes = new CopyOnWriteArraySet<>();

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

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
        registerConverter(NativeConverter.BOOLEAN);
        registerConverter(NativeConverter.BOOLEAN_TYPE);
        registerConverter(NativeConverter.BIG_DECIMAL);
        registerConverter(NativeConverter.BIG_INTEGER);
        registerConverter(NativeConverter.BYTE);
        registerConverter(NativeConverter.BYTE_TYPE);
        registerConverter(NativeConverter.DATE);
        registerConverter(NativeConverter.DOUBLE);
        registerConverter(NativeConverter.DOUBLE_TYPE);
        registerConverter(NativeConverter.FLOAT);
        registerConverter(NativeConverter.FLOAT_TYPE);
        registerConverter(NativeConverter.INET);
        registerConverter(NativeConverter.INTEGER);
        registerConverter(NativeConverter.INTEGER_TYPE);
        registerConverter(NativeConverter.LONG);
        registerConverter(NativeConverter.LONG_TYPE);
        registerConverter(NativeConverter.SHORT);
        registerConverter(NativeConverter.SHORT_TYPE);
        registerConverter(NativeConverter.STRING);
        registerConverter(NativeConverter.TIMESTAMP);
        registerConverter(NativeConverter.UUID);
        registerConverter(NativeConverter.BLOB);

        registerConverter(new EnumConverterProvider());
        registerConverter(new ByteArrayConverter());

        registerConverter(new JavaDurationConverter());
        registerConverter(new JavaInstantConverter());
        registerConverter(new JavaLocalDateConverter());
        registerConverter(new JavaLocalDateTimeConverter());
        registerConverter(new JavaLocalTimeConverter());
        registerConverter(new JavaOffsetDateTimeConverter());
        registerConverter(new JavaOffsetTimeConverter());
        registerConverter(new JavaPeriodConverter());

        serviceStream(Converter.class).forEach(this::registerConverter);
        serviceStream(ConverterProvider.class).forEach(this::registerConverter);
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

    public final void registerConverter(Converter converter) {
        registerConverter(new ConstantConverterProvider(converter));
    }

    public final void registerConverter(ConverterProvider provider) {
        Class<?> valueType = provider.getValueType();
        LOGGER.debug("Adding provider {} -> {}...", valueType.getCanonicalName(), provider.converterType().getCanonicalName());
        providers.put(valueType, provider);
    }

    protected final <T> Stream<T> serviceStream(Class<T> serviceClass) {
        return StreamSupport.stream(ServiceLoader.load(serviceClass).spliterator(), false);
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

}
