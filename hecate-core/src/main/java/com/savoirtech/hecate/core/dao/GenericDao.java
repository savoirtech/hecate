package com.savoirtech.hecate.core.dao;

import java.util.List;
import java.util.Set;

public interface GenericDao<KeyType, T> {

    public void delete(KeyType key);

    public Set getKeys();

    public boolean containsKey(KeyType key);

    public void save(KeyType key, T pojo);

    public T find(KeyType key);

    public Set<T> findItems(final List<KeyType> keys, final String rangeFrom, final String rangeTo);
}


