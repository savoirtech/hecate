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

package com.savoirtech.hecate.pojo.binding.facet;

import java.util.LinkedList;
import java.util.List;

import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.ColumnBinding;
import com.savoirtech.hecate.pojo.binding.column.NestedColumnBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.convert.PojoInstanceConverter;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;

public class EmbeddedFacetBinding extends NestedColumnBinding<ColumnBinding> implements ColumnBinding {
//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public EmbeddedFacetBinding(Facet parent, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        super(createBindings(parent, converterRegistry, namingStrategy));
    }

    private static List<ColumnBinding> createBindings(Facet parent, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        List<ColumnBinding> bindings = new LinkedList<>();
        bindings.add(new SimpleFacetBinding(parent, namingStrategy.getColumnName(parent), new PojoInstanceConverter(parent.getType().getRawType())));
        parent.subFacets(false).stream().forEach(sub -> {
            Converter converter = converterRegistry.getConverter(sub.getType());
            if(converter == null) {
                throw new HecateException("No converter found for facet \"%s\" of type %s.", sub.getName(), sub.getType().getRawType().getCanonicalName());
            }
            bindings.add(new SimpleFacetBinding(sub, namingStrategy.getColumnName(sub), converter));
        });
        return bindings;
    }
}
