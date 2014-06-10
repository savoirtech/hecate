package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoFindByKeys extends PojoPersistenceStatement {

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKeys.class);

    public PojoFindByKeys(Session session, PojoMapping mapping) {
        super(session, createSelect(mapping), mapping);
    }

    private static Select.Where createSelect(PojoMapping mapping) {
        final Select.Where where = pojoSelect(mapping)
                .where(in(mapping.getIdentifierMapping().getFacetMetadata().getColumnName(), bindMarker()));
        LOGGER.info("{}.findByKeys(): {}", mapping.getPojoMetadata().getPojoType().getSimpleName(), where);
        return where;
    }

    public List<Object> execute(Map<Object, Object> pojos, QueryContext queryContext) {
        return list(pojos, executeWithArgs(cassandraValues(pojos.keySet())), queryContext);
    }

    public List<Object> execute(Iterable<Object> identifiers, QueryContext queryContext) {
        return execute(getPojoMapping().getPojoMetadata().newPojoMap(identifiers), queryContext);
    }

    private List<Object> cassandraValues(Iterable<Object> keys) {
        List<Object> cassandraValues = new LinkedList<>();
        for (Object key : keys) {
            cassandraValues.add(getPojoMapping().getIdentifierMapping().getColumnHandler().getWhereClauseValue(key));
        }
        return cassandraValues;
    }
}
