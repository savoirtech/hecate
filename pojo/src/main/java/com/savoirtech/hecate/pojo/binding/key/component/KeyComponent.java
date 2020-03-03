/*
 * Copyright (c) 2012-2016 Savoir Technologies, Inc.
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

package com.savoirtech.hecate.pojo.binding.key.component;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

public interface KeyComponent extends ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    Facet getFacet();
    DataType getDataType();
    Delete delete(OngoingWhereClause<Delete> delete);
    int getOrder();
    int getRank();
    String getColumnName();
    Object toColumnValue(Object facetValue);
    ColumnBinding createReferenceBinding(Facet referencingFacet, NamingStrategy strategy);
}
