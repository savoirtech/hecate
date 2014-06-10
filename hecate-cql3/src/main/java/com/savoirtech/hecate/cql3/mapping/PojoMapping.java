package com.savoirtech.hecate.cql3.mapping;

import com.savoirtech.hecate.cql3.meta.PojoMetadata;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class PojoMapping {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final List<FacetMapping> facetMappings = new LinkedList<>();

    private final PojoMetadata pojoMetadata;
    private final String tableName;
    private FacetMapping identifierMapping;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping(PojoMetadata pojoMetadata, String tableName) {
        this.pojoMetadata = pojoMetadata;
        this.tableName = tableName == null ? pojoMetadata.getTableName() : tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public FacetMapping getIdentifierMapping() {
        return identifierMapping;
    }

    public PojoMetadata getPojoMetadata() {
        return pojoMetadata;
    }

    public String getTableName() {
        return tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void addFacet(FacetMapping facetMapping) {
        facetMappings.add(facetMapping);
        if (facetMapping.getFacetMetadata().isIdentifier()) {
            identifierMapping = facetMapping;
        }
    }

    public List<FacetMapping> getFacetMappings() {
        return Collections.unmodifiableList(facetMappings);
    }
}
