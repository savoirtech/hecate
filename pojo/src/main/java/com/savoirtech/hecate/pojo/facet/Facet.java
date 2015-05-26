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

import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Index;
import com.savoirtech.hecate.pojo.util.GenericType;
import com.savoirtech.hecate.pojo.util.PojoUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.lang.annotation.Annotation;
import java.util.List;

public interface Facet {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Facet flatten();
    
    <A extends Annotation> A getAnnotation(Class<A> annotationType);

    default String getColumnName() {
        Column column = Validate.notNull(this).getAnnotation(Column.class);
        return column != null ? column.value() : PojoUtils.underscoreSeparated(getName());
    }
    
    default String getIndexName() {
        Index annot = getAnnotation(Index.class);
        if (annot != null && StringUtils.isNotEmpty(annot.value())) {
            return annot.value();
        }
        return getName() + "_ndx";
    }

    String getName();

    GenericType getType();

    Object getValue(Object pojo);

    <A extends Annotation> boolean hasAnnotation(Class<A> annotationType);

    default boolean isCascadeDelete() {
        Cascade cascade = getAnnotation(Cascade.class);
        return cascade == null || cascade.delete();
    }

    default boolean isCascadeSave() {
        Cascade cascade = getAnnotation(Cascade.class);
        return cascade == null || cascade.save();
    }

    default boolean isIndexed() {
        return hasAnnotation(Index.class);
    }

    void setValue(Object pojo, Object value);

    List<Facet> subFacets(boolean allowNullParent);
}
