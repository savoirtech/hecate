package com.savoirtech.hecate.cql3.persistence;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.savoirtech.hecate.cql3.meta.PojoDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoFindByKeys<P, K> extends PojoPersistenceStatement<P> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKeys.class);

    public PojoFindByKeys(Session session, String tableName, PojoDescriptor<P> pojoDescriptor) {
        super(session, createSelect(tableName, pojoDescriptor), pojoDescriptor);
    }

    private static <P> Select.Where createSelect(String tableName, PojoDescriptor<P> pojoDescriptor) {
        final Select.Where where = pojoSelect(pojoDescriptor)
                .from(tableName)
                .where(in(pojoDescriptor.getIdentifierMapping().getColumnName(), bindMarker()));

        LOGGER.info("{}.findByKeys(): {}", pojoDescriptor.getPojoType().getSimpleName(), where);
        return where;
    }

    public List<P> execute(Iterable<K> keys) {
        return list(executeWithArgs(cassandraValues(keys)));
    }

    private List<Object> cassandraValues(Iterable<K> keys) {
        List<Object> cassandraValues = new LinkedList<>();
        for (K key : keys) {
            cassandraValues.add(identifierMapping().getConverter().toCassandraValue(key));
        }
        return cassandraValues;
    }
}
