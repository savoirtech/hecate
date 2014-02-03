/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.core.utils;

import java.lang.reflect.Field;
import java.util.Iterator;

import com.savoirtech.hecate.core.annotations.CompositeComponentProcessor;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnIterator<KeyType, NameType, ValueType> implements Iterator<Column> {

    static private Logger LOG = LoggerFactory.getLogger(ColumnIterator.class);
    private final Class<NameType> nameClass;
    private final Class<ValueType> valueClass;
    protected ColumnSliceIterator<KeyType, Object, byte[]> csi;
    protected Field[] fields = null;

    public ColumnIterator(SliceQuery<KeyType, Object, byte[]> sliceQuery, final Class<NameType> nameClass, final Class<ValueType> valueClass,
                          Object start, Object finish, boolean reversed) {
        this.nameClass = nameClass;
        this.valueClass = valueClass;
        fields = CompositeComponentProcessor.checkSetComposite(nameClass);
        csi = new ColumnSliceIterator<KeyType, Object, byte[]>(sliceQuery, start, finish, reversed);
    }

    public ColumnIterator(SliceQuery<KeyType, Object, byte[]> sliceQuery, final Class<NameType> nameClass, final Class<ValueType> valueClass,
                          Object start, Object finish, boolean reversed, int count) {
        this.nameClass = nameClass;
        this.valueClass = valueClass;
        fields = CompositeComponentProcessor.checkSetComposite(nameClass);
        csi = new ColumnSliceIterator<KeyType, Object, byte[]>(sliceQuery, start, finish, reversed, count);
    }

    @Override
    public boolean hasNext() {
        return csi.hasNext();
    }

    @Override
    public Column<NameType, ValueType> next() {
        HColumn<Object, byte[]> hcol = csi.next();
        NameType name;
        if (fields != null) {
            Composite comp = (Composite) hcol.getName();
            name = unmarshalComposite(comp);
        } else {
            name = (NameType) hcol.getName();
        }
        ValueType val = (ValueType) SerializerTypeInferer.getSerializer(valueClass).fromBytes(hcol.getValue());
        Column<NameType, ValueType> col = new Column<NameType, ValueType>(name, val);
        return col;
    }

    @Override
    public void remove() {
        csi.remove();
    }

    protected NameType unmarshalComposite(Composite comp) {

        try {
            NameType name = nameClass.newInstance();

            for (int i = 0;i < fields.length;i++) {
                fields[i].set(name, comp.get(i, SerializerTypeInferer.getSerializer(fields[i].getType())));
            }

            return name;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
