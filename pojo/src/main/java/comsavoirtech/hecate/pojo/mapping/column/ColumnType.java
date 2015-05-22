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

import java.util.function.Function;

public interface ColumnType<C,F> {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    C getColumnValue(F facetValue, Function<Object, Object> function);

    DataType getDataType(DataType elementDataType);
    
    F getFacetValue(C columnValue, Function<Object,Object> function, Class<?> elementType);
    
    Iterable<Object> facetElements(F facetValue);

    Iterable<Object> columnElements(C columnValue);
}
