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

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.binding.FacetBinding;
import com.savoirtech.hecate.pojo.binding.column.NestedColumnBinding;
import com.savoirtech.hecate.pojo.convert.Converter;
import com.savoirtech.hecate.pojo.convert.ConverterRegistry;
import com.savoirtech.hecate.pojo.facet.Facet;
import com.savoirtech.hecate.pojo.facet.FacetProvider;
import com.savoirtech.hecate.pojo.facet.SubFacet;
import com.savoirtech.hecate.pojo.naming.NamingStrategy;
import com.savoirtech.hecate.pojo.reflect.ReflectionUtils;

public class EmbeddedFacetBinding extends NestedColumnBinding implements FacetBinding {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Facet facet;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public EmbeddedFacetBinding(Facet parent, FacetProvider facetProvider, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        super(createBindings(parent, facetProvider, converterRegistry, namingStrategy));
        this.facet = parent;
    }

    private static List<FacetBinding> createBindings(Facet parent, FacetProvider facetProvider, ConverterRegistry converterRegistry, NamingStrategy namingStrategy) {
        List<FacetBinding> bindings = new LinkedList<>();
        bindings.add(new SimpleFacetBinding(parent, namingStrategy.getColumnName(parent), new NullIndicatorConverter(parent.getType().getRawType())));
        facetProvider.getFacets(parent.getType().getRawType()).stream().map(child -> new SubFacet(parent, child, false)).forEach(sub -> {
            Converter converter = converterRegistry.getConverter(sub.getType());
            if(converter == null) {
                throw new HecateException("No converter found for facet \"%s\" of type %s.", sub.getName(), sub.getType().getRawType().getCanonicalName());
            }
            bindings.add(new SimpleFacetBinding(sub, namingStrategy.getColumnName(sub), converter));
        });
        return bindings;
    }

//----------------------------------------------------------------------------------------------------------------------
// FacetBinding Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public Facet getFacet() {
        return facet;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private static class NullIndicatorConverter implements Converter {
        private final Class<?> pojoType;

        public NullIndicatorConverter(Class<?> pojoType) {
            this.pojoType = pojoType;
        }

        @Override
        public DataType getDataType() {
            return DataType.cboolean();
        }

        @Override
        public Class<?> getValueType() {
            return Boolean.class;
        }


        @Override
        public Object toColumnValue(Object value) {
            return value != null;
        }

        @Override
        public Object toFacetValue(Object value) {
            return Boolean.TRUE.equals(value) ? ReflectionUtils.newInstance(pojoType) : null;
        }
    }
}
