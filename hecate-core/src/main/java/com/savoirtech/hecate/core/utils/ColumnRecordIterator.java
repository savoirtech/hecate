/*
 * Copyright (c) 2012-2014 Savoir Technologies, Inc.
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

import com.savoirtech.hecate.core.indexing.CompositeColumnIdentifier;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.SliceQuery;

public class ColumnRecordIterator extends ColumnIterator {

    public ColumnRecordIterator(SliceQuery sliceQuery, Class nameClass, Class valueClass, Object start, Object finish, boolean reversed) {
        super(sliceQuery, nameClass, valueClass, start, finish, reversed);
    }

    public ColumnRecordIterator(SliceQuery sliceQuery, Class nameClass, Class valueClass, Object start, Object finish, boolean reversed, int count) {
        super(sliceQuery, nameClass, valueClass, start, finish, reversed, count);
    }

    @Override
    protected Object unmarshalComposite(Composite comp) {

        try {

            CompositeColumnIdentifier name = new CompositeColumnIdentifier();
            Integer pos = 0;

            for (int i = 0; i < comp.size(); i++) {
                name.addIdentifier(String.valueOf(i), comp.get(i, StringSerializer.get()));
            }

            return name;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Column<CompositeColumnIdentifier, String> next() {
        HColumn<Object, byte[]> hcol = csi.next();
        CompositeColumnIdentifier name;
        Composite comp = (Composite) hcol.getName();
        name = (CompositeColumnIdentifier) unmarshalComposite(comp);
        String val = SerializerTypeInferer.getSerializer(String.class).fromBytes(hcol.getValue()).toString();
        Column<CompositeColumnIdentifier, String> col = new Column<CompositeColumnIdentifier, String>(name, val);
        return col;
    }
}
