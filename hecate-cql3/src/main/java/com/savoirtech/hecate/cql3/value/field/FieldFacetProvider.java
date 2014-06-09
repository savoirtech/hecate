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
