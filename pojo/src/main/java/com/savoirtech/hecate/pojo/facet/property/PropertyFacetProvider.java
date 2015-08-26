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

package com.savoirtech.hecate.pojo.facet.property;

import java.beans.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.savoirtech.hecate.annotation.Ignored;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import org.apache.commons.lang3.StringUtils;

public class PropertyFacetProvider implements FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final String IS_PREFIX = "is";
    private static final String GET_PREFIX = "get";
    private static final String SET_PREFIX = "set";

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static Method accessible(Method method) {
        if (method != null) {
            method.setAccessible(true);
        }
        return method;
    }

    static List<Facet> facetsOf(Class<?> pojoClass) {
        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(pojoClass);
            PropertyDescriptor[] descriptors = beanInfo.getPropertyDescriptors();
            final List<Facet> facets = new ArrayList<>(descriptors.length);
            for (PropertyDescriptor descriptor : descriptors) {
                final Method readMethod = getReadMethod(pojoClass, descriptor);
                final Method writeMethod = getWriteMethod(pojoClass, descriptor);
                if (readMethod != null &&
                        writeMethod != null &&
                        !readMethod.isAnnotationPresent(Transient.class) &&
                        !readMethod.isAnnotationPresent(Ignored.class)) {
                    facets.add(new PropertyFacet(pojoClass, descriptor, readMethod, writeMethod));
                }
            }
            return facets;
        } catch (IntrospectionException e) {
            throw new HecateException(e, "Unable to introspect class %s.", pojoClass.getCanonicalName());
        }
    }

    private static Method findDeclaredMethod(Class<?> c, String name, Class<?>... parameterTypes) {
        try {
            return c.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            if (c.getSuperclass() != null) {
                return findDeclaredMethod(c.getSuperclass(), name, parameterTypes);
            }
            return null;
        }
    }

    private static Method getReadMethod(Class<?> pojoType, PropertyDescriptor descriptor) {
        Method method = descriptor.getReadMethod();
        if (method == null) {
            Method candidate = findDeclaredMethod(pojoType, getReadMethodName(descriptor));
            if (candidate != null && descriptor.getPropertyType().equals(candidate.getReturnType())) {
                method = candidate;
            }
        }
        return accessible(method);
    }

    private static String getReadMethodName(PropertyDescriptor descriptor) {
        final String prefix = Boolean.TYPE.equals(descriptor.getPropertyType()) ? IS_PREFIX : GET_PREFIX;
        return prefix + propertySuffix(descriptor);
    }

    private static Method getWriteMethod(Class<?> pojoType, PropertyDescriptor descriptor) {
        Method method = descriptor.getWriteMethod();
        if (method == null) {
            Method candidate = findDeclaredMethod(pojoType, getWriteMethodName(descriptor), descriptor.getPropertyType());
            if (candidate != null && Void.TYPE.equals(candidate.getReturnType())) {
                method = candidate;
            }
        }
        return accessible(method);
    }

    private static String getWriteMethodName(PropertyDescriptor descriptor) {
        return SET_PREFIX + propertySuffix(descriptor);
    }

    private static String propertySuffix(PropertyDescriptor descriptor) {
        return StringUtils.capitalize(descriptor.getName());
    }

//----------------------------------------------------------------------------------------------------------------------
// FacetProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Facet> getFacets(Class<?> pojoClass) {
        return facetsOf(pojoClass);
    }
}
