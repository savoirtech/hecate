package com.savoirtech.hecate.cql3.handler.pojo;

import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.AbstractListHandler;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.persistence.DeleteContext;
import com.savoirtech.hecate.cql3.persistence.QueryContext;
import com.savoirtech.hecate.cql3.persistence.SaveContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    public void getDeletionIdentifiers(Object cassandraValue, DeleteContext context) {
        if (cassandraValue != null) {
            List<Object> cassandraValues = (List<Object>) cassandraValue;
            Set<Object> identifiers = new HashSet<>();
            if (!cassandraValues.isEmpty()) {
                for (Object value : cassandraValues) {
                    identifiers.add(identifierConverter.fromCassandraValue(value));
                }
                context.addDeletedIdentifiers(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers);
            }
        }
    }

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
    protected void onFacetValueComplete(List<Object> facetValues, QueryContext context) {
        if (!CollectionUtils.isEmpty(facetValues)) {
            List<Object> identifiers = new ArrayList<>(facetValues.size());
            for (Object pojo : facetValues) {
                identifiers.add(pojoMetadata.getIdentifierFacet().getFacet().get(pojo));
            }
            context.addPojos(pojoMetadata.getPojoType(), facetMetadata.getTableName(), pojoMetadata.newPojoMap(identifiers));
        }
    }

    @Override
    protected Object toCassandraElement(Object facetElement, SaveContext context) {
        final Object identifierValue = pojoMetadata.getIdentifierFacet().getFacet().get(facetElement);
        context.addPojo(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifierValue, facetElement);
        return identifierConverter.toCassandraValue(identifierValue);
    }

    @Override
    protected Object toFacetElement(Object cassandraElement, QueryContext context) {
        return pojoMetadata.newPojo(identifierConverter.fromCassandraValue(cassandraElement));
    }
}
