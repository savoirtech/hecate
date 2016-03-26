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

import java.util.List;
import java.util.stream.Collectors;

import com.savoirtech.hecate.pojo.binding.KeyBinding;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

public class CompositeKeyBinding extends AbstractCompositeKeyBinding implements KeyBinding {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CompositeKeyBinding(List<Facet> keyFacets, NamingStrategy namingStrategy, ConverterRegistry converterRegistry) {
        super(keyFacets.stream().map(facet -> createComponent(facet, converterRegistry, namingStrategy)).collect(Collectors.toList()));
    }
}