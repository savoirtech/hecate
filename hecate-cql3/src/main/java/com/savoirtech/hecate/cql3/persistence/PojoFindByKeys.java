package com.savoirtech.hecate.cql3.persistence;

public class PojoFindByKeys<P, K> { //extends PojoPersistenceStatement<P> {

//    private static final Logger LOGGER = LoggerFactory.getLogger(PojoFindByKeys.class);
//
//    public PojoFindByKeys(Session session, String tableName, PojoDescriptor<P> pojoDescriptor) {
//        super(session, createSelect(tableName, pojoDescriptor), pojoDescriptor);
//    }
//
//    private static <P> Select.Where createSelect(String tableName, PojoDescriptor<P> pojoDescriptor) {
//        final Select.Where where = pojoSelect(pojoDescriptor)
//                .from(tableName)
//                .where(in(pojoDescriptor.getIdentifierMapping().getColumnName(), bindMarker()));
//
//        LOGGER.info("{}.findByKeys(): {}", pojoDescriptor.getPojoType().getSimpleName(), where);
//        return where;
//    }
//
//    public List<P> execute(Iterable<K> keys) {
//        return list(executeWithArgs(cassandraValues(keys)));
//    }
//
//    private List<Object> cassandraValues(Iterable<K> keys) {
//        List<Object> cassandraValues = new LinkedList<>();
//        for (K key : keys) {
//            cassandraValues.add(identifierMapping().getConverter().toCassandraValue(key, null));
//        }
//        return cassandraValues;
//    }
}
