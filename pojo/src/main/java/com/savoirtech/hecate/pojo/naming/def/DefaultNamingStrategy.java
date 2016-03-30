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

package com.savoirtech.hecate.pojo.naming.def;

import java.util.Arrays;
import java.util.stream.Collectors;

import com.savoirtech.hecate.annotation.Column;
import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.annotation.UDT;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import org.apache.commons.lang3.StringUtils;

public class DefaultNamingStrategy implements NamingStrategy {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    public static String underscoreSeparated(String camelCaseName) {
        return StringUtils.lowerCase(
                Arrays.stream(StringUtils.split(camelCaseName, SubFacet.SEPARATOR))
                        .flatMap(component -> Arrays.stream(StringUtils.splitByCharacterTypeCamelCase(component)))
                        .collect(Collectors.joining("_")));
    }

//----------------------------------------------------------------------------------------------------------------------
// NamingStrategy Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String getColumnName(Facet facet) {
        Column column = facet.getAnnotation(Column.class);
        return column == null ? underscoreSeparated(facet.getName()) : column.value();
    }

    public String getReferenceTableName(Facet facet) {
        Table table = facet.getAnnotation(Table.class);
        return table == null ? getTableName(facet.getType().getElementType().getRawType()) : table.value();
    }

    @Override
    public String getTableName(Class<?> pojoClass) {
        Table table = pojoClass.getAnnotation(Table.class);
        return table != null ? table.value() : underscoreSeparated(pojoClass.getSimpleName());
    }

    @Override
    public String getUserTypeName(Facet facet) {
        UDT udt = facet.getAnnotation(UDT.class);
        return StringUtils.isEmpty(udt.value()) ? underscoreSeparated(facet.getType().getRawType().getSimpleName()) : udt.value();
    }
}
