package com.savoirtech.hecate.core.dao;

import java.util.List;
import java.util.Set;

public interface ColumnFamilyDao<K, T> {

    public void delete(K key);

    public Set<K> getKeys();

    public boolean containsKey(K key);

    public void save(K key, T pojo);

    public T find(K key);

    public Set<T> findItems(final List<K> keys, final String rangeFrom, final String rangeTo);
}


