/*
 * Copyright (c) 2012-2015 Savoir Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.pojo.mapping;

import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.annotation.Ttl;
import com.savoirtech.hecate.core.exception.HecateException;
import org.apache.commons.lang3.Validate;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PojoMapping<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private final Class<P> pojoClass;
    private final String tableName;
    private final List<ScalarFacetMapping> idMappings;
    private final List<FacetMapping> simpleMappings;
    private final int ttl;
    private final boolean cascadeDelete;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static <A extends Annotation, M extends FacetMapping> Stream<M> annotatedWith(List<M> mappings, Class<A> annotationType) {
        return mappings.stream().filter(mapping -> mapping.getFacet().hasAnnotation(annotationType));
    }

    private static ClusteringColumn clusteringColumn(FacetMapping mapping) {
        return mapping.getFacet().getAnnotation(ClusteringColumn.class);
    }

    public static int getTtl(Class<?> pojoClass) {
        Ttl ttl = Validate.notNull(pojoClass).getAnnotation(Ttl.class);
        return ttl != null ? ttl.value() : 0;
    }

    private static PartitionKey partitionKey(FacetMapping mapping) {
        return mapping.getFacet().getAnnotation(PartitionKey.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping(Class<P> pojoClass, String tableName, List<ScalarFacetMapping> idMappings, List<FacetMapping> simpleMappings) {
        this.pojoClass = pojoClass;
        this.tableName = tableName;
        this.idMappings = sorted(idMappings);
        this.simpleMappings = simpleMappings.stream().sorted((left, right) -> left.getColumnName().compareTo(right.getColumnName())).collect(Collectors.toList());
        this.ttl = getTtl(pojoClass);
        this.cascadeDelete = simpleMappings.stream().filter(FacetMapping::isReference).filter(mapping -> !mapping.getFacet().hasAnnotation(Cascade.class) || mapping.getFacet().getAnnotation(Cascade.class).delete()).findFirst().isPresent();
    }

    private static List<ScalarFacetMapping> sorted(List<ScalarFacetMapping> idMappings) {
        if (idMappings.size() > 1) {
            List<ScalarFacetMapping> sorted = new ArrayList<>(idMappings.size());
            sorted.addAll(annotatedWith(idMappings, PartitionKey.class).sorted((left, right) -> partitionKey(left).order() - partitionKey(right).order()).collect(Collectors.toList()));
            sorted.addAll(annotatedWith(idMappings, ClusteringColumn.class).sorted((left, right) -> clusteringColumn(left).order() - clusteringColumn(right).order()).collect(Collectors.toList()));
            return sorted;
        }
        return idMappings;
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Class<P> getPojoClass() {
        return pojoClass;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTtl() {
        return ttl;
    }

    public boolean isCascadeDelete() {
        return cascadeDelete;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        return pojoClass.getSimpleName() + "@" + tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public ScalarFacetMapping getForeignKeyMapping() {
        if (idMappings.size() == 1) {
            return idMappings.get(0);
        } else {
            throw new HecateException("Class %s contains a composite primary key (%s) and does not support foreign key references.", pojoClass.getSimpleName(), idMappings.stream().map(mapping -> mapping.getFacet().getName()).collect(Collectors.joining(",")));
        }
    }

    public List<ScalarFacetMapping> getIdMappings() {
        return Collections.unmodifiableList(idMappings);
    }

    public List<FacetMapping> getSimpleMappings() {
        return Collections.unmodifiableList(simpleMappings);
    }
}
