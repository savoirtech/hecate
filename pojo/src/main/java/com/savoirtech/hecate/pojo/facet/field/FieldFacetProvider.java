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
import com.savoirtech.hecate.pojo.facet.FacetProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FieldFacetProvider implements FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static void collectFields(Class<?> type, List<Field> fields) {
        final Field[] declaredFields = type.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            if (isPersistable(declaredField)) {
                fields.add(declaredField);
            }
        }
        if (type.getSuperclass() != null) {
            collectFields(type.getSuperclass(), fields);
        }
    }

    static List<Facet> facetsOf(Class<?> pojoType) {
        final List<Field> fields = getFields(pojoType);
        return fields.stream().filter(FieldFacetProvider::isPersistable).map(field -> new FieldFacet(pojoType, field)).collect(Collectors.toList());
    }

    private static List<Field> getFields(Class<?> pojoType) {
        List<Field> fields = new LinkedList<>();
        collectFields(pojoType, fields);
        return fields;
    }

    private static boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !Modifier.isTransient(mods) && !Modifier.isStatic(mods);
    }

//----------------------------------------------------------------------------------------------------------------------
// FacetProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Facet> getFacets(Class<?> pojoType) {
        return facetsOf(pojoType);
    }
}
