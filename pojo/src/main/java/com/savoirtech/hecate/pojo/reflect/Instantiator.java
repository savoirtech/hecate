package com.savoirtech.hecate.pojo.reflect;

@FunctionalInterface
public interface Instantiator<T> {
    T instantiate() throws ReflectiveOperationException;
}
