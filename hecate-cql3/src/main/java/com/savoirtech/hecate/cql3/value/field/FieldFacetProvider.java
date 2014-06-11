/*
 * Copyright (c) 2014. Savoir Technologies
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

package com.savoirtech.hecate.cql3.value.field;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class FieldFacetProvider implements FacetProvider {
//----------------------------------------------------------------------------------------------------------------------
// ValueProvider Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public List<Facet> getFacets(Class<?> pojoType) {
        final List<Field> fields = ReflectionUtils.getFields(pojoType);
        final List<Facet> facets = new ArrayList<>();
        for (Field field : fields) {
            if (isPersistable(field)) {
                facets.add(new FieldFacet(pojoType, field));
            }
        }
        return facets;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    private boolean isPersistable(Field field) {
        final int mods = field.getModifiers();
        return !(Modifier.isFinal(mods) ||
                Modifier.isTransient(mods) ||
                Modifier.isStatic(mods));
    }
}
