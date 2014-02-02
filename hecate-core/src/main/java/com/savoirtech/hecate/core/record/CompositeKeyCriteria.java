package com.savoirtech.hecate.core.record;

import java.util.LinkedList;
import java.util.List;

public class CompositeKeyCriteria {

    private List<CompositeColumnIdentifier> criteriaPermutations = new LinkedList<CompositeColumnIdentifier>();

    public void addIdentifier(CompositeColumnIdentifier compositeColumnIdentifier) {
        criteriaPermutations.add(compositeColumnIdentifier);
    }

    public List<CompositeColumnIdentifier> getCriteriaPermutations() {
        return criteriaPermutations;
    }
}
