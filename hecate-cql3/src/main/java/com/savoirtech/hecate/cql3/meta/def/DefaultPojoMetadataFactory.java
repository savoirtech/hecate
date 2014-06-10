package com.savoirtech.hecate.cql3.meta.def;

import com.google.common.collect.MapMaker;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.value.Facet;
import com.savoirtech.hecate.cql3.value.FacetProvider;
import com.savoirtech.hecate.cql3.value.field.FieldFacetProvider;
import org.apache.commons.lang3.Validate;

import java.util.Map;

public class DefaultPojoMetadataFactory implements PojoMetadataFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Map<Class<?>, PojoMetadata> pojoMetadatas;
    private FacetProvider facetProvider = new FieldFacetProvider();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMetadataFactory() {
        pojoMetadatas = new MapMaker().makeMap();
    }

    public DefaultPojoMetadataFactory(int concurrencyLevel) {
        pojoMetadatas = new MapMaker().concurrencyLevel(concurrencyLevel).makeMap();
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMetadataFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public PojoMetadata getPojoMetadata(Class<?> pojoType) {
        PojoMetadata pojoMetadata = pojoMetadatas.get(pojoType);
        if (pojoMetadata == null) {
            pojoMetadata = new PojoMetadata(pojoType);
            for (Facet facet : facetProvider.getFacets(pojoType)) {
                pojoMetadata.addFacet(new FacetMetadata(facet));
            }
            pojoMetadatas.put(pojoType, pojoMetadata);
        }
        Validate.isTrue(pojoMetadata.getIdentifierFacet() != null, "Invalid POJO type %s (no identifier found).", pojoType.getCanonicalName());
        return pojoMetadata;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setFacetProvider(FacetProvider facetProvider) {
        this.facetProvider = facetProvider;
    }
}
