package com.savoirtech.hecate.cql3.meta;

import com.savoirtech.hecate.cql3.ReflectionUtils;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PojoDescriptor<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<P> pojoType;
    private final List<FacetMapping> facetMappings = new LinkedList<>();
    private FacetMapping identifierMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDescriptor(Class<P> pojoType) {
        this.pojoType = pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping getIdentifierMapping() {
        return identifierMapping;
    }

    public Class<P> getPojoType() {
        return pojoType;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addMapping(FacetMapping facetMapping) {
        if (facetMapping.isIdentifier()) {
            this.identifierMapping = facetMapping;
            facetMappings.add(0, facetMapping);
        } else {
            facetMappings.add(facetMapping);
        }
    }

    public FacetMapping facet(String name) {
        for (FacetMapping mapping : facetMappings) {
            if (name.equals(mapping.getFacet().getName())) {
                return mapping;
            }
        }
        return null;
    }

    public List<FacetMapping> getFacetMappings() {
        return Collections.unmodifiableList(facetMappings);
    }

    public P newInstance() {
        return ReflectionUtils.instantiate(pojoType);
    }
}
