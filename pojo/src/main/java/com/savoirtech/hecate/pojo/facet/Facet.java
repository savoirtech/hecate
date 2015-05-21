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

public interface Facet {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    <A extends Annotation> A getAnnotation(Class<A> annotationType);

    default String getColumnName() {
        return PojoUtils.getColumnName(this);
    }
    
    String getName();

    GenericType getType();

    Object getValue(Object pojo);

    default <A extends Annotation> boolean hasAnnotation(Class<A> annotationType) {
        return getAnnotation(annotationType) != null;
    }

    void setValue(Object pojo, Object value);

    List<Facet> subFacets();
}
