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

package com.savoirtech.hecate.pojo.binding;

import java.util.List;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

public interface KeyBinding extends ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    ColumnBinding createReferenceBinding(Facet parent, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy);
    KeyComponent createClusteringColumnReferenceComponent(Facet parent, PojoBinding<?> pojoBinding, NamingStrategy namingStrategy);
    void delete(Delete.Where delete);
    boolean isNullElement(Object element);
    List<Object> elementToKeys(Object element);
    DataType getElementDataType();
    Object getElementValue(Object pojo);

    List<Object> getKeyParameters(List<Object> keys);

    void selectWhere(Select.Where select);
}
