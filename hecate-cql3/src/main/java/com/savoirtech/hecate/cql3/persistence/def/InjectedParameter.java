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

package com.savoirtech.hecate.cql3.persistence.def;

import org.apache.commons.lang3.builder.CompareToBuilder;

import java.util.List;

public class InjectedParameter implements Comparable<InjectedParameter> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Object parameter;
    private final int index;

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public InjectedParameter(int index, Object parameter) {
        this.index = index;
        this.parameter = parameter;
    }

//----------------------------------------------------------------------------------------------------------------------
// Comparable Implementation
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public int compareTo(InjectedParameter other) {
        return new CompareToBuilder().append(index, other.index).build();
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InjectedParameter that = (InjectedParameter) o;

        if (index != that.index) {
            return false;
        }
        if (parameter != null ? !parameter.equals(that.parameter) : that.parameter != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = parameter != null ? parameter.hashCode() : 0;
        result = 31 * result + index;
        return result;
    }

    public String toString() {
        return parameter + " @ " + index;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public void injectInto(List<Object> parameters) {
        parameters.add(index, parameter);
    }
}
