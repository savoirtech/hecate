package com.savoirtech.hecate.cql3.handler.pojo;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractArrayHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

public class PojoArrayHandler extends AbstractArrayHandler {

    private final FacetMetadata facetMetadata;
    private final PojoMetadata pojoMetadata;
    private final ValueConverter identifierConverter;

    public PojoArrayHandler(FacetMetadata facetMetadata, PojoMetadata pojoMetadata, ValueConverter identifierConverter) {
        super(pojoMetadata.getPojoType(), identifierConverter.getDataType());
        this.facetMetadata = facetMetadata;
        this.pojoMetadata = pojoMetadata;
        this.identifierConverter = identifierConverter;
    }

    @Override
    protected Object toCassandraElement(Object facetElement, SaveContext context) {
        final Object identifierValue = pojoMetadata.getIdentifierFacet().getFacet().get(facetElement);
        context.enqueue(pojoMetadata.getPojoType(), facetMetadata.getTableName(), facetElement);
        return identifierConverter.toCassandraValue(identifierValue);
    }

    @Override
    protected Object toFacetElement(Object cassandraElement, QueryContext context) {
        Object pojo = pojoMetadata.newPojo();
        context.addPojo(pojoMetadata.getPojoType(), facetMetadata.getTableName(), cassandraElement, pojo);
        return pojo;
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return identifierConverter.toCassandraValue(parameterValue);
    }
}
