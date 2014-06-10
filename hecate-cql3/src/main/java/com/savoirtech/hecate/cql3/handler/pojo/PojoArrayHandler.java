package com.savoirtech.hecate.cql3.handler.pojo;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractArrayHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.persistence.DeleteContext;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PojoArrayHandler extends AbstractArrayHandler {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facetMetadata;
    private final PojoMetadata pojoMetadata;
    private final ValueConverter identifierConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoArrayHandler(FacetMetadata facetMetadata, PojoMetadata pojoMetadata, ValueConverter identifierConverter) {
        super(pojoMetadata.getPojoType(), identifierConverter.getDataType());
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

    @Override
    public boolean isCascading() {
        return true;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    protected void onFacetValueComplete(Object facetValue, QueryContext context) {
        final int length = Array.getLength(facetValue);
        final Map<Object, Object> pojos = new HashMap<>();
        for (int i = 0; i < length; ++i) {
            Object pojo = Array.get(facetValue, i);
            pojos.put(pojoMetadata.getIdentifierFacet().getFacet().get(pojo), pojo);
        }
        context.addPojos(pojoMetadata.getPojoType(), facetMetadata.getTableName(), pojos);
    }

    @Override
    protected Object toCassandraElement(Object facetElement, SaveContext context) {
        final Object identifierValue = pojoMetadata.getIdentifierFacet().getFacet().get(facetElement);
        Object pojo = pojoMetadata.newPojo(identifierValue);
        context.addPojo(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifierValue, pojo);
        return identifierConverter.toCassandraValue(identifierValue);
    }

    @Override
    protected Object toFacetElement(Object cassandraElement, QueryContext context) {
        return pojoMetadata.newPojo(identifierConverter.fromCassandraValue(cassandraElement));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void getDeletionIdentifiers(Object cassandraValue, DeleteContext context) {
        if (cassandraValue != null) {
            List<Object> cassandraValues = (List<Object>) cassandraValue;
            if (!cassandraValues.isEmpty()) {
                Set<Object> identifiers = new HashSet<>();
                for (Object value : cassandraValues) {
                    identifiers.add(identifierConverter.fromCassandraValue(value));
                }
                context.addDeletedIdentifiers(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers);
            }
        }
    }
}
