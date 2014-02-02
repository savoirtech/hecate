package com.savoirtech.hecate.core.annotations;

import java.lang.reflect.Field;
import java.util.Collection;

public class CollectionProcessor {

    public static void createCollection(Field field) {

        if (Collection.class.isAssignableFrom(field.getType())) {

            //We have a collection.
            Collection collection;
            try {
                collection = (Collection) field.get(field.getDeclaringClass());
                if (collection.size() > 0) {
                    //We have values

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();  //TODO
            }
        }
    }
}
