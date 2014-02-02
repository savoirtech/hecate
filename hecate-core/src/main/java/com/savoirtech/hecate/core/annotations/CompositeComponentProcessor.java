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
