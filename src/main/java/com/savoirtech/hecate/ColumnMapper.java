package com.savoirtech.hecate;

import me.prettyprint.hector.api.beans.HColumn;

import java.util.List;
import java.util.Set;

public interface ColumnMapper<N,V> {

    Set<HColumn<N,?>> toColumns(V object);

    void fromColumns(V object, List<HColumn<N, byte[]>> columns);
}
