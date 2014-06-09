package com.savoirtech.hecate.cql3.meta.def;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.convert.ValueConverterRegistry;
import com.savoirtech.hecate.cql3.convert.def.DefaultValueConverterRegistry;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import com.savoirtech.hecate.cql3.meta.PojoDescriptorFactory;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;
import com.savoirtech.hecate.cql3.value.field.FieldFacetProvider;

public class DefaultPojoDescriptorFactory implements PojoDescriptorFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private ValueConverterRegistry valueConverterRegistry = DefaultValueConverterRegistry.defaultRegistry();
    private FacetProvider facetProvider = new FieldFacetProvider();

//----------------------------------------------------------------------------------------------------------------------
// PojoDescriptorFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public <P> PojoDescriptor<P> getPojoDescriptor(Class<P> pojoType) {
        PojoDescriptor<P> descriptor = new PojoDescriptor<>(pojoType);
        for (Facet facet : facetProvider.getFacets(pojoType)) {
            ValueConverter converter = valueConverterRegistry.getValueConverter(facet.getType());
            descriptor.addMapping(new FacetMapping(facet, converter));
        }
        return descriptor;
    }
}
