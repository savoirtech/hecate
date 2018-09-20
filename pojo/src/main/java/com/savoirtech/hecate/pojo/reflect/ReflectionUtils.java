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

package com.savoirtech.hecate.pojo.reflect;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Function;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.savoirtech.hecate.core.exception.HecateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Optional.ofNullable;

public class ReflectionUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionUtils.class);

    private static LoadingCache<Class<?>, Instantiator<?>> INSTANTIATOR_CACHE = CacheBuilder.newBuilder().build(new CacheLoader<Class<?>, Instantiator<?>>() {
        @Override
        public Instantiator<?> load(Class<?> key) {
            return createInstantiator(key);
        }
    });

    private static Supplier<Function<Class<?>, Object>> unsafeInstantiator = Suppliers.memoize(() -> {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final Object unsafe = f.get(null);
            final Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
            return type -> invokeAllocateInstance(unsafe, allocateInstance, type);
        } catch (ReflectiveOperationException e) {
            LOGGER.warn("Unable to use sun.misc.Unsafe class to instantiate objects!", e);
            return null;
        }
    });

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static <T> Instantiator<T> createInstantiator(Class<T> pojoClass) {
        try {
            final Constructor<T> ctor = pojoClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor::newInstance;
        } catch (NoSuchMethodException e) {
            final Function<Class<?>, Object> fn = ofNullable(unsafeInstantiator.get())
                    .orElseThrow(() -> new HecateException(e, "Unable to instantiate object of type %s (no-arg constructor missing).", pojoClass.getCanonicalName()));
            return () -> pojoClass.cast(fn.apply(pojoClass));
        }
    }

    private static Object invokeAllocateInstance(Object unsafe, Method allocateInstance, Class<?> type) {
        try {
            return allocateInstance.invoke(unsafe, type);
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to allocate %s instance.", type.getCanonicalName());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> pojoClass) {
        try {
            final Instantiator<?> instantiator = INSTANTIATOR_CACHE.getUnchecked(pojoClass);
            return pojoClass.cast(instantiator.instantiate());
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to instantiate object of type %s.", pojoClass.getCanonicalName());
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private ReflectionUtils() {

    }
}
