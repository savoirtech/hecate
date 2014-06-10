package com.savoirtech.hecate.cql3.mapping.def;

import com.savoirtech.hecate.cql3.handler.ColumnHandlerFactory;
import com.savoirtech.hecate.cql3.handler.def.DefaultColumnHandlerFactory;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMappingFactory;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadataFactory;
import com.savoirtech.hecate.cql3.meta.def.DefaultPojoMetadataFactory;

public class DefaultPojoMappingFactory implements PojoMappingFactory {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private PojoMetadataFactory pojoMetadataFactory = new DefaultPojoMetadataFactory();
    private ColumnHandlerFactory columnHandlerFactory = new DefaultColumnHandlerFactory();

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public DefaultPojoMappingFactory() {
        this.pojoMetadataFactory = new DefaultPojoMetadataFactory();
        final DefaultColumnHandlerFactory handlerFactory = new DefaultColumnHandlerFactory();
        handlerFactory.setPojoMetadataFactory(pojoMetadataFactory);
        this.columnHandlerFactory = handlerFactory;
    }

//----------------------------------------------------------------------------------------------------------------------
// PojoMappingFactory Implementation
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping getPojoMapping(Class<?> pojoType, String tableName) {
        PojoMetadata pojoMetadata = pojoMetadataFactory.getPojoMetadata(pojoType);
        final PojoMapping pojoMapping = new PojoMapping(pojoMetadata, tableName);
        for (FacetMetadata facet : pojoMetadata.getFacets().values()) {
            pojoMapping.addFacet(new FacetMapping(facet, columnHandlerFactory.getColumnHandler(facet)));
        }
        return pojoMapping;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public void setPojoMetadataFactory(PojoMetadataFactory pojoMetadataFactory) {
        this.pojoMetadataFactory = pojoMetadataFactory;
    }
}
