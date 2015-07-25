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

package com.savoirtech.hecate.pojo.convert.time;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.convert.Converter;

import java.time.Period;

public class JavaPeriodConverter implements Converter {
//----------------------------------------------------------------------------------------------------------------------
// Converter Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public DataType getDataType() {
        return DataType.varchar();
    }

    @Override
    public Class<?> getValueType() {
        return Period.class;
    }

    @Override
    public Object toColumnValue(Object value) {
        if(value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public Object toFacetValue(Object value) {
        if(value == null) {
            return null;
        }
        return Period.parse(value.toString());
    }
}
