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

package com.savoirtech.hecate.pojo.facet.field;

import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.facet.reflect.ReflectionFacet;
import com.savoirtech.hecate.pojo.type.GenericType;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.util.List;
import java.util.stream.Collectors;

public class FieldFacet extends ReflectionFacet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(FieldFacet.class);

    private final Field field;
    private final GenericType type;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public FieldFacet(Class<?> pojoClass, Field field) {
        this.field = Validate.notNull(field, "Field cannot be null.");
        field.setAccessible(true);
        this.type = new GenericType(pojoClass, field.getGenericType());
    }

//----------------------------------------------------------------------------------------------------------------------
// Facet Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String getName() {
        return field.getName();
    }

    @Override
    public GenericType getType() {
        return type;
    }

    @Override
    public List<Facet> subFacets(boolean allowNullParent) {
        return FieldFacetProvider.facetsOf(type.getRawType()).stream().map(facet -> new SubFacet(this, facet, allowNullParent)).collect(Collectors.toList());
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected AccessibleObject getAnnotationSource() {
        return field;
    }

    @Override
    public Object getValueReflectively(Object pojo) throws ReflectiveOperationException {
        return field.get(pojo);
    }

    @Override
    public void setValueReflectively(Object pojo, Object value) throws ReflectiveOperationException {
        field.set(pojo, value);
    }
}
