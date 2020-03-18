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

package com.savoirtech.hecate.pojo.facet.reflect;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.facet.Facet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;

public abstract class ReflectionFacet implements Facet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Logger logger = LoggerFactory.getLogger(getClass());

//----------------------------------------------------------------------------------------------------------------------
// Abstract Methods
//----------------------------------------------------------------------------------------------------------------------

    protected abstract AccessibleObject getAnnotationSource();

    protected abstract Object getValueReflectively(Object pojo) throws ReflectiveOperationException;

    protected abstract void setValueReflectively(Object pojo, Object value) throws ReflectiveOperationException;

//----------------------------------------------------------------------------------------------------------------------
// Facet Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Facet flatten() {
        return this;
    }

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return getAnnotationSource().getAnnotation(annotationType);
    }

    @Override
    public Object getValue(Object pojo) {
        try {
            return getValueReflectively(pojo);
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to get value for facet %s.", getName());
        }
    }

    @Override
    public <A extends Annotation> boolean hasAnnotation(Class<A> annotationType) {
        return getAnnotationSource().isAnnotationPresent(annotationType);
    }

    @Override
    public void setValue(Object pojo, Object value) {
        try {
            logger.debug("Setting {} to value '{}'.", this.getName(), value);
            setValueReflectively(pojo, value);
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to set value for facet %s.", getName());
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        return getName();
    }
}
