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

package com.savoirtech.hecate.pojo.binding.key.composite;

import java.util.stream.Collectors;

import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.binding.key.component.KeyComponent;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

public class CompositeKeyObjectBinding extends AbstractCompositeKeyBinding implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static KeyComponent createComponent(Facet parent, Facet child, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        return createComponent(new SubFacet(parent, child, true), converterRegistry, namingStrategy);
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CompositeKeyObjectBinding(Facet parent, FacetProvider facetProvider, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        super(facetProvider.getFacets(parent.getType().getRawType()).stream().map(child -> createComponent(parent, child, converterRegistry, namingStrategy)).collect(Collectors.toList()));
    }
}