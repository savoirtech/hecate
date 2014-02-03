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
