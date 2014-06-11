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

package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.util.GenericType;
import com.savoirtech.hecate.cql3.value.Facet;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;

public class FieldFacet implements Facet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Field field;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FieldFacet(Class<?> pojoType, Field field) {
        this.field = field;
        this.type = new GenericType(pojoType, field.getGenericType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Value Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    @SuppressWarnings("unchecked")
    public Object get(Object pojo) {
        return ReflectionUtils.getFieldValue(field, pojo);
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return field.getAnnotation(annotationType);
    }

    @Override
    public String getName() {
        return field.getName();
    }

    public GenericType getType() {
        return type;
    }

    @Override
    public void set(Object pojo, Object value) {
        ReflectionUtils.setFieldValue(field, pojo, value);
    }
}
