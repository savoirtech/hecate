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

package com.savoirtech.hecate.pojo.util;

import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.annotation.Ttl;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PojoUtils {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static String getColumnName(Facet facet) {
        Column column = Validate.notNull(facet).getAnnotation(Column.class);
        return column != null ? column.value() : underscoreSeparated(facet.getName());
    }

    public static String getTableName(Class<?> pojoClass) {
        Table table = Validate.notNull(pojoClass).getAnnotation(Table.class);
        return table != null ? table.value() : underscoreSeparated(pojoClass.getSimpleName());
    }

    public static String getTableName(Facet facet) {
        Table table = Validate.notNull(facet).getAnnotation(Table.class);
        return table != null ? table.value() : getTableName(facet.getType().getRawType());
    }

    public static int getTtl(Class<?> pojoClass) {
        Ttl ttl = Validate.notNull(pojoClass).getAnnotation(Ttl.class);
        return ttl != null ? ttl.value() : 0;
    }

    public static boolean isCascadeDelete(Facet facet) {
        Cascade cascade = facet.getAnnotation(Cascade.class);
        return cascade == null || cascade.delete();
    }

    public static boolean isCascadeSave(Facet facet) {
        Cascade cascade = facet.getAnnotation(Cascade.class);
        return cascade == null || cascade.save();
    }

    public static <T> T newPojo(Class<T> pojoClass) {
        try {
            Constructor<T> constructor = pojoClass.getConstructor();
            constructor.setAccessible(true);
            return Validate.notNull(pojoClass).newInstance();
        } catch (ReflectiveOperationException e) {
            throw new HecateException(e, "Unable to instantiate object of type %s.", pojoClass.getCanonicalName());
        }
    }

    private static String underscoreSeparated(String camelCaseName) {
        String[] words = StringUtils.splitByCharacterTypeCamelCase(camelCaseName);
        List<String> wordsList = new ArrayList<>(Arrays.asList(words));
        wordsList.remove(SubFacet.SEPARATOR);
        return StringUtils.lowerCase(StringUtils.join(wordsList, "_"));
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    private PojoUtils() {
        // Prevent instantiation!
    }
}
