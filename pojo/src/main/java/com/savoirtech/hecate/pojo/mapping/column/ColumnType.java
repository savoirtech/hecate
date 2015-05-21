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
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.persistence.Dehydrator;
import com.savoirtech.hecate.pojo.persistence.Hydrator;

public interface ColumnType {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Object convertParameterValue(Object facetValue);

    DataType getDataType();

    Object getInsertValue(Dehydrator dehydrator, Object facetValue);

    void setFacetValue(Hydrator hydrator, Object pojo, Facet facet, Object cassandraValue);
}
