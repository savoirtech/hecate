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

package com.savoirtech.hecate.pojo.convert.binary;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.pojo.convert.Converter;

import java.nio.ByteBuffer;

public class ByteArrayConverter implements Converter {
//----------------------------------------------------------------------------------------------------------------------
// Converter Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Object fromCassandraValue(Object value) {
        if(value == null) {
            return null;
        }
        ByteBuffer buff = (ByteBuffer)value;
        byte[] bytes = new byte[buff.remaining()];
        buff.get(bytes);
        return bytes;
    }

    @Override
    public DataType getDataType() {
        return DataType.blob();
    }

    @Override
    public Object toCassandraValue(Object value) {
        return value == null ? null : ByteBuffer.wrap((byte[]) value);
    }
}