package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.savoirtech.hecate.cql3.handler.context.SaveContext;
import com.savoirtech.hecate.cql3.mapping.FacetMapping;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PojoSave extends PojoPersistenceStatement {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoSave.class);

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoSave(Session session, PojoMapping mapping) {
        super(session, createInsert(mapping), mapping);
    }

    private static Insert createInsert(PojoMapping mapping) {
        final Insert insert = QueryBuilder.insertInto(mapping.getTableName());
        for (FacetMapping facetMapping : mapping.getFacetMappings()) {
            insert.value(facetMapping.getFacetMetadata().getColumnName(), QueryBuilder.bindMarker());
        }
        insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        LOGGER.info("{}.save(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), insert);
        return insert;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void execute(Object pojo, SaveContext saveContext) {
        List<Object> parameters = new ArrayList<>(getPojoMapping().getFacetMappings().size());
        for (FacetMapping mapping : getPojoMapping().getFacetMappings()) {
            Object facetValue = mapping.getFacetMetadata().getFacet().get(pojo);
            parameters.add(mapping.getColumnHandler().getInsertValue(facetValue, saveContext));
        }
        parameters.add(getPojoMapping().getPojoMetadata().getDefaultTtl());
        executeWithList(parameters);
    }
}
