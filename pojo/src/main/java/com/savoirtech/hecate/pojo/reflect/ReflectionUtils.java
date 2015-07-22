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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.savoirtech.hecate.core.exception.HecateException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Function;

public class ReflectionUtils {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static Supplier<Function<Class<?>, Object>> unsafeInstantiator = Suppliers.memoize(() -> {
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field f = unsafeClass.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            final Object unsafe = f.get(null);
            final Method allocateInstance = unsafeClass.getMethod("allocateInstance", Class.class);
            return (type) -> {
                try {
                    return allocateInstance.invoke(unsafe, type);
                } catch (ReflectiveOperationException e) {
                    throw new HecateException(e, "Unable to allocate %s instance.", type.getCanonicalName());
                }
            };
        } catch (ReflectiveOperationException e) {
            return null;
        }
    });

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> pojoClass) {
        try {
            try {
                Constructor<T> constructor = pojoClass.getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor.newInstance();
            } catch (NoSuchMethodException e) {
                Function<Class<?>,Object> fn = unsafeInstantiator.get();
                if (fn == null) {
                    throw new HecateException("Unable to instantiate object of type %s (no-arg constructor missing).", pojoClass.getCanonicalName());
                }
                return (T) fn.apply(pojoClass);
            }
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
