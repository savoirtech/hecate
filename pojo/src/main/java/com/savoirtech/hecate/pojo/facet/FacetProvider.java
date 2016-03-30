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

package com.savoirtech.hecate.pojo.facet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.savoirtech.hecate.core.exception.HecateException;

public interface FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    List<Facet> getFacets(Class<?> pojoClass);

    default Map<String, Facet> getFacetsAsMap(Class<?> pojoClass) {
        return getFacets(pojoClass).stream().collect(Collectors.toMap(Facet::getName, facet -> facet, (left, right) -> {
            throw new HecateException("Duplicate facet \"%s\" detected.", left.getName());
        }));
    }
}
