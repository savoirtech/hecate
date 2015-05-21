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

import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.facet.reflect.ReflectionFacet;
import com.savoirtech.hecate.pojo.util.GenericType;
import org.apache.commons.lang3.Validate;

import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

public class PropertyFacet extends ReflectionFacet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final PropertyDescriptor propertyDescriptor;
    private final Method readMethod;
    private final Method writeMethod;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PropertyFacet(Class<?> pojoType, PropertyDescriptor propertyDescriptor, Method readMethod, Method writeMethod) {
        this.propertyDescriptor = Validate.notNull(propertyDescriptor, "Property descriptor cannot be null.");
        this.readMethod = readMethod;
        this.writeMethod = writeMethod;
        this.type = new GenericType(pojoType, readMethod.getGenericReturnType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Facet Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String getName() {
        return propertyDescriptor.getName();
    }

    @Override
    public GenericType getType() {
        return type;
    }

    @Override
    public List<Facet> subFacets(boolean allowNullParent) {
        return PropertyFacetProvider.facetsOf(type.getRawType()).stream().map(facet -> new SubFacet(this, facet, allowNullParent)).collect(Collectors.toList());
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected AccessibleObject getAnnotationSource() {
        return readMethod;
    }

    @Override
    public Object getValueReflectively(Object pojo) throws ReflectiveOperationException {
        return readMethod.invoke(Validate.notNull(pojo));
    }

    @Override
    public void setValueReflectively(Object pojo, Object value) throws ReflectiveOperationException {
        writeMethod.invoke(pojo, value);
    }
}
