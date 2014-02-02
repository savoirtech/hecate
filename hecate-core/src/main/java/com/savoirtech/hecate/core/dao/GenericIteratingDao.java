package com.savoirtech.hecate.core.dao;

import java.util.Set;

import com.savoirtech.hecate.core.utils.ColumnIterator;

public interface GenericIteratingDao<KeyType, NameType, ValueType> {

    public void delete(KeyType key);

    public void deleteColumn(KeyType key, NameType name);

    public Set getKeys();

    public boolean containsKey(KeyType key);

    public void save(KeyType key, NameType name, ValueType value);

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key);

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, boolean reverse);

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, NameType start, NameType end);

    public ColumnIterator<KeyType, NameType, ValueType> find(KeyType key, NameType start, NameType end, boolean reverse);
}
