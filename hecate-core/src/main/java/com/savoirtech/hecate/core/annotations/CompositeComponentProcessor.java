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

package com.savoirtech.hecate.core.annotations;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

public class CompositeComponentProcessor {

    public static Field[] checkSetComposite(Class clazz) {

        Field[] declaredFields = clazz.getDeclaredFields();
        ArrayList<Field> fieldList = new ArrayList<Field>();
        for (Field field : declaredFields) {
            field.setAccessible(true);
            //  if (field.isAnnotationPresent(CompositeComponent.class)) {
            fieldList.add(field);
            // }
        }

        if (fieldList.size() == 0) {
            return null;
        }

        Field[] fields = fieldList.toArray(new Field[fieldList.size()]);

        Arrays.sort(fields, new Comparator<Field>() {
            @Override
            public int compare(Field o1, Field o2) {
                CompositeComponent or1 = o1.getAnnotation(CompositeComponent.class);
                CompositeComponent or2 = o2.getAnnotation(CompositeComponent.class);
                // nulls last
                if (or1 != null && or2 != null) {
                    return or1.order() - or2.order();
                } else {
                    if (or1 != null && or2 == null) {
                        return -1;
                    } else {
                        if (or1 == null && or2 != null) {
                            return 1;
                        }
                    }
                }
                return o1.getName().compareTo(o2.getName());
            }
        });

        return fields;
    }
}
