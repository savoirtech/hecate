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

package com.savoirtech.hecate.pojo.facet;

import com.savoirtech.hecate.pojo.util.GenericType;
import com.savoirtech.hecate.pojo.util.PojoUtils;

import java.lang.annotation.Annotation;
import java.util.List;

public class SubFacet implements Facet {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final String SEPARATOR = ".";
    private final Facet parent;
    private final Facet child;
    private final boolean createParent;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public SubFacet(Facet parent, Facet child) {
        this(parent, child, false);
    }
    
    public SubFacet(Facet parent, Facet child, boolean createParent) {
        this.parent = parent;
        this.child = child;
        this.createParent = createParent;
    }

//----------------------------------------------------------------------------------------------------------------------
// Facet Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <A extends Annotation> A getAnnotation(Class<A> annotationType) {
        return child.getAnnotation(annotationType);
    }

    @Override
    public String getName() {
        return parent.getName() + SEPARATOR + child.getName();
    }

    @Override
    public GenericType getType() {
        return child.getType();
    }

    @Override
    public Object getValue(Object pojo) {
        Object parentValue = parentValue(pojo);
        return parentValue == null ? null : child.getValue(parentValue);
    }

    @Override
    public void setValue(Object pojo, Object value) {
        Object parentValue = parentValue(pojo);
        if (parentValue != null) {
            child.setValue(parentValue, value);
        }
    }

    @Override
    public List<Facet> subFacets() {
        return child.subFacets();
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private Object parentValue(Object pojo) {
        Object value = parent.getValue(pojo);
        if (createParent && value == null) {
            value = PojoUtils.newPojo(parent.getType().getRawType());
            parent.setValue(pojo, value);
        }
        return value;
    }
}
