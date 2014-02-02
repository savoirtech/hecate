package com.savoirtech.hecate.core.utils;

public class Column<K, V> {

    private K name;
    private V value;

    public Column() {
    }

    public Column(K name, V value) {
        this.name = name;
        this.value = value;
    }

    public K getName() {
        return name;
    }

    public void setName(K name) {
        this.name = name;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
