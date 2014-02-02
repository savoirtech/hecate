package com.savoirtech.hecate.core.utils;

import com.savoirtech.hecate.core.record.CompositeColumnIdentifier;
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

            for (int i = 0;i < comp.size();i++) {
                name.addIdentifier(String.valueOf(i), comp.get(i, StringSerializer.get()));
            }

            return name;
        } catch (Exception e) {
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
