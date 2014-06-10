package com.savoirtech.hecate.cql3.handler.pojo;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractListHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class PojoListHandler extends AbstractListHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facetMetadata;
    private final PojoMetadata pojoMetadata;
    private final ValueConverter identifierConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoListHandler(FacetMetadata facetMetadata, PojoMetadata pojoMetadata, ValueConverter identifierConverter) {
        super(identifierConverter.getDataType());
        this.facetMetadata = facetMetadata;
        this.pojoMetadata = pojoMetadata;
        this.identifierConverter = identifierConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandler Implementation
//----------------------------------------------------------------------------------------------------------------------


    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return identifierConverter.toCassandraValue(parameterValue);
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected Object toCassandraElement(Object facetElement, SaveContext context) {
        final Object identifierValue = pojoMetadata.getIdentifierFacet().getFacet().get(facetElement);
        context.enqueue(pojoMetadata.getPojoType(), facetMetadata.getTableName(), facetElement);
        return identifierConverter.toCassandraValue(identifierValue);
    }

    @Override
    protected void onFacetValueComplete(List<Object> facetValues, QueryContext context) {
        if (!CollectionUtils.isEmpty(facetValues)) {
            List<Object> identifiers = new ArrayList<Object>(facetValues.size());
            for (Object pojo : facetValues) {
                identifiers.add(pojoMetadata.getIdentifierFacet().getFacet().get(pojo));
            }
            context.addPojos(pojoMetadata.getPojoType(), facetMetadata.getTableName(), pojoMetadata.newPojoMap(identifiers));
        }
    }

    @Override
    protected Object toFacetElement(Object cassandraElement, QueryContext context) {
        return pojoMetadata.newPojo(identifierConverter.fromCassandraValue(cassandraElement));
    }
}
