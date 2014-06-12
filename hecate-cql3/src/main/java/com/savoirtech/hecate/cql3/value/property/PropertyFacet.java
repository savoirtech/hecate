/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Facet;

import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class PropertyFacet implements Facet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PropertyDescriptor propertyDescriptor;
    private final Method readMethod;
    private final Method writeMethod;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PropertyFacet(Class<?> declaringClass, PropertyDescriptor propertyDescriptor, Method readMethod, Method writeMethod) {
        this.propertyDescriptor = propertyDescriptor;
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.type = new GenericType(declaringClass, readMethod.getGenericReturnType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Facet Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object get(Object pojo) {
        return ReflectionUtils.invoke(pojo, readMethod);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return readMethod.getAnnotation(annotationType);
    }

    @Override
    public String getName() {
        return propertyDescriptor.getName();
    }

    @Override
    @SuppressWarnings("unchecked")
    public GenericType getType() {
        return type;
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.invoke(pojo, writeMethod, value);
    }
}
