package com.savoirtech.hecate;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.List;
import java.util.Set;

public class CassandraDao<K, N, V> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Keyspace keyspace;
    private final String columnFamily;
    private final Class<K> keyType;
    private final Class<N> columnNameType;
    private final Class<V> valueType;
    private final ColumnMapper<N,V> mapper;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public CassandraDao(Keyspace keyspace, String columnFamily, Class<K> keyType, Class<N> columnNameType, Class<V> valueType, ColumnMapper<N,V> mapper) {
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.keyType = keyType;
        this.columnNameType = columnNameType;
        this.valueType = valueType;
        this.mapper = mapper;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void delete(K key) {
        mutator().delete(key, columnFamily, null, getSerializer(columnNameType));
    }

    public V find(K key) {
        SliceQuery<K, N, byte[]> query = HFactory.createSliceQuery(keyspace, getSerializer(keyType), getSerializer(columnNameType), BytesArraySerializer.get());

        final QueryResult<ColumnSlice<N, byte[]>> result = query.setColumnFamily(columnFamily).setKey(key).setRange(null, null, false, Integer.MAX_VALUE).execute();

        final List<HColumn<N, byte[]>> columns = result.get().getColumns();
        if (columns.isEmpty()) {
            return null;
        }
        V value = newValue();
        mapper.fromColumns(value, columns);
        return value;
    }

    protected <T> Serializer<T> getSerializer(Class<T> type) {
        return SerializerTypeInferer.getSerializer(type);
    }

    protected V newValue() {
        try {
            return valueType.newInstance();
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to instantiate a new value object.", e);
        }
        catch (InstantiationException e) {
            throw new RuntimeException("Unable to instantiate a new value object.", e);
        }
    }

    public void save(K key, V value) {
        Mutator<K> mutator = mutator();
        final Set<HColumn<N, ?>> columns = mapper.toColumns(value);
        for (HColumn<?, ?> column : columns) {
            mutator.addInsertion(key, columnFamily, column);
        }
        mutator.execute();
    }

    private Mutator<K> mutator() {
        return HFactory.createMutator(keyspace, getSerializer(keyType));
    }
}
