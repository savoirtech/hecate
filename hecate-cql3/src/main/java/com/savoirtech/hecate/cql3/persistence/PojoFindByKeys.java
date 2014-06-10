package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.mapping.PojoMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
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
        final List<Object> identifiers = cassandraIdentifiers(pojos.keySet());
        if (!identifiers.isEmpty()) {
            return list(pojos, executeWithArgs(identifiers), queryContext);
        }
        return Collections.emptyList();
    }

    public List<Object> execute(Iterable<Object> identifiers, QueryContext queryContext) {
        if (identifiers.iterator().hasNext()) {
            return execute(getPojoMapping().getPojoMetadata().newPojoMap(identifiers), queryContext);
        }
        return Collections.emptyList();
    }

}
