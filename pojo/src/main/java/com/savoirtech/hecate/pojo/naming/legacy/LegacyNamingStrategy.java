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

package com.savoirtech.hecate.pojo.naming.legacy;

import com.savoirtech.hecate.annotation.Table;
import com.savoirtech.hecate.annotation.UDT;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import org.apache.commons.lang3.StringUtils;

public class LegacyNamingStrategy implements NamingStrategy {
//----------------------------------------------------------------------------------------------------------------------
// NamingStrategy Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public String getColumnName(Facet facet) {
        return facet.getName();
    }

    @Override
    public String getReferenceTableName(Facet facet) {
        Table table = facet.getAnnotation(Table.class);
        return table == null ? getTableName(facet.getType().getElementType().getRawType()) : table.value();
    }

    @Override
    public String getTableName(Class<?> pojoClass) {
        Table table = pojoClass.getAnnotation(Table.class);
        return table != null ? table.value() : pojoClass.getSimpleName();
    }

    @Override
    public String getUserTypeName(Facet facet) {
        UDT udt = facet.getAnnotation(UDT.class);
        return StringUtils.isEmpty(udt.value()) ? facet.getType().getRawType().getSimpleName() : udt.value();
    }
}
