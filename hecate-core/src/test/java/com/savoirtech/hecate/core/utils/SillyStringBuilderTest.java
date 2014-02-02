package com.savoirtech.hecate.core.utils;

import com.savoirtech.hecate.core.record.CompositeColumnIdentifier;
import org.junit.Test;


import static org.junit.Assert.assertTrue;

public class SillyStringBuilderTest {

    @Test
    public void testUTF8String() {

        CompositeColumnIdentifier indexer = new CompositeColumnIdentifier();

        indexer.addIdentifier("A", "a");
        indexer.addIdentifier("B", "b");
        indexer.addIdentifier("C", "c");

        StringBuilder builder = new StringBuilder();
        builder.append("(");
        for (String s : indexer.getMap().values()) {
            builder.append("UTF8Type, ");
        }

        builder.replace(builder.lastIndexOf(","), builder.lastIndexOf(" ") + 1, ")");

        String built = builder.toString();
        String expected = "(UTF8Type, UTF8Type, UTF8Type)";

        System.out.println(built + " = " + expected);
        assertTrue(built.equals(expected));
    }
}
