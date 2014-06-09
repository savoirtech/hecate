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
