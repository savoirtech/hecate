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

package com.savoirtech.hecate.pojo.mapping.column;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.util.PojoUtils;

import java.util.function.Function;

public class EmbeddedColumnType implements ColumnType<Boolean, Object> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    public static final EmbeddedColumnType INSTANCE = new EmbeddedColumnType();

//----------------------------------------------------------------------------------------------------------------------
// ColumnType Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Iterable<Object> columnElements(Boolean columnValue) {
        throw new HecateException("Embedded object columns are not reference-capable.");
    }

    @Override
    public Iterable<Object> facetElements(Object facetValue) {
        throw new HecateException("Embedded object columns are not reference-capable.");
    }

    @Override
    public Boolean getColumnValue(Object facetValue, Function<Object, Object> function) {
        return true;
    }

    @Override
    public DataType getDataType(DataType elementDataType) {
        return DataType.cboolean();
    }

    @Override
    public Object getFacetValue(Boolean columnValue, Function<Object, Object> function, Class<?> elementType) {
        return PojoUtils.newPojo(elementType);
    }
}
