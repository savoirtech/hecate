package com.savoirtech.hecate.cql3.handler.delegate;

import com.datastax.driver.core.DataType;
import com.savoirtech.hecate.cql3.convert.ValueConverter;
import com.savoirtech.hecate.cql3.handler.context.DeleteContext;
import com.savoirtech.hecate.cql3.handler.context.QueryContext;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.meta.FacetMetadata;
import com.savoirtech.hecate.cql3.meta.PojoMetadata;
import com.savoirtech.hecate.cql3.util.InjectionTarget;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PojoDelegate implements ColumnHandlerDelegate {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final FacetMetadata facetMetadata;
    private final PojoMetadata pojoMetadata;
    private final ValueConverter identifierConverter;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoDelegate(PojoMetadata pojoMetadata, FacetMetadata facetMetadata, ValueConverter identifierConverter) {
        this.pojoMetadata = pojoMetadata;
        this.facetMetadata = facetMetadata;
        this.identifierConverter = identifierConverter;
    }

//----------------------------------------------------------------------------------------------------------------------
// ColumnHandlerDelegate Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public void collectDeletionIdentifiers(Iterable<Object> columnValues, DeleteContext context) {
        final Set<Object> identifiers = new HashSet<>();
        for (Object columnValue : columnValues) {
            identifiers.add(identifierConverter.fromCassandraValue(columnValue));
        }
        context.addDeletedIdentifiers(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers);
    }

    @Override
    public Object convertToInsertValue(Object facetValue, SaveContext saveContext) {
        final Object identifier = identifierConverter.toCassandraValue(pojoMetadata.getIdentifierFacet().getFacet().get(facetValue));
        saveContext.addPojo(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifier, facetValue);
        return identifier;
    }

    @Override
    public DataType getDataType() {
        return identifierConverter.getDataType();
    }

    @Override
    public Object getWhereClauseValue(Object parameterValue) {
        return identifierConverter.toCassandraValue(parameterValue);
    }

    @Override
    public void injectFacetValues(InjectionTarget<Map<Object, Object>> target, Iterable<Object> columnValues, QueryContext context) {
        Set<Object> identifiers = new HashSet<>();
        for (Object columnValue : columnValues) {
            identifiers.add(identifierConverter.fromCassandraValue(columnValue));
        }
        context.addPojos(pojoMetadata.getPojoType(), facetMetadata.getTableName(), identifiers, new PojoInjectionTarget(target));
    }

    @Override
    public boolean isCascading() {
        return true;
    }

//----------------------------------------------------------------------------------------------------------------------
// Inner Classes
//----------------------------------------------------------------------------------------------------------------------

    private class PojoInjectionTarget implements InjectionTarget<List<Object>> {
        private final InjectionTarget<Map<Object, Object>> target;

        private PojoInjectionTarget(InjectionTarget<Map<Object, Object>> target) {
            this.target = target;
        }

        @Override
        public void inject(List<Object> pojos) {
            Map<Object, Object> columnValueToPojo = new HashMap<>();
            for (Object pojo : pojos) {
                Object identifier = pojoMetadata.getIdentifierFacet().getFacet().get(pojo);
                columnValueToPojo.put(identifierConverter.toCassandraValue(identifier), pojo);
            }
            target.inject(columnValueToPojo);
        }
    }
}
