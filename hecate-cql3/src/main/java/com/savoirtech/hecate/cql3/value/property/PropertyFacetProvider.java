/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.cql3.value.property;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;
import org.apache.commons.beanutils.PropertyUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class PropertyFacetProvider implements FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Facet> getFacets(Class<?> pojoType) {
        final PropertyDescriptor[] descriptors = PropertyUtils.getPropertyDescriptors(pojoType);
        final List<Facet> facets = new ArrayList<>(descriptors.length);
        for (PropertyDescriptor descriptor : descriptors) {
            final Method readMethod = ReflectionUtils.getReadMethod(pojoType, descriptor);
            final Method writeMethod = ReflectionUtils.getWriteMethod(pojoType, descriptor);
            if (readMethod != null && writeMethod != null) {
                facets.add(new PropertyFacet(pojoType, descriptor, readMethod, writeMethod));
            }
        }
        return facets;
    }
}
