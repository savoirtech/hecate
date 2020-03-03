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

package com.savoirtech.hecate.pojo.convert;

import com.datastax.oss.driver.api.core.type.DataType;

public interface Converter {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    /**
     * Returns the {@link DataType} to be used by this converter
     * @return the data type
     */
    DataType getDataType();

    /**
     * Returns the Java value type used by this converter.
     * @return the value type
     */
    Class<?> getValueType();

    /**
     * Converts a facet value to a column value.
     * @param value the facet value
     * @return the corresponding column value
     */
    Object toColumnValue(Object value);

    /**
     * Converts a column value to a facet value.
     * @param value the column value
     * @return the corresponding facet value
     */
    Object toFacetValue(Object value);
}
