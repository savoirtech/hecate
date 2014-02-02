package com.savoirtech.hecate.core.record;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

public class CompositeColumnIdentifier implements Serializable {

    //Natural sorted Map
    private Map columns = new LinkedHashMap<String, String>();

    public void addIdentifier(Object k, Object v) {
        columns.put(k, v);
    }

    public Map<String, String> getMap() {
        return columns;
    }

    @Override
    public String toString() {
        return "CompositeColumnIdentifier{" +
            "columns=" + columns +
            '}';
    }
}
