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
