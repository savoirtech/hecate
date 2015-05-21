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

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class PojoMapping<P> {
//----------------------------------------------------------------------------------------------------------------------
// Fields
//----------------------------------------------------------------------------------------------------------------------

    private static final Logger LOGGER = LoggerFactory.getLogger(PojoMapping.class);

    private final Class<P> pojoClass;
    private final String tableName;
    private final List<FacetMapping> idMappings;
    private final List<FacetMapping> simpleMappings;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static <A extends Annotation> Stream<FacetMapping> annotatedWith(List<FacetMapping> mappings, Class<A> annotationType) {
        return mappings.stream().filter(mapping -> mapping.getFacet().hasAnnotation(annotationType));
    }

    private static ClusteringColumn clusteringColumn(FacetMapping mapping) {
        return mapping.getFacet().getAnnotation(ClusteringColumn.class);
    }

    private static PartitionKey partitionKey(FacetMapping mapping) {
        return mapping.getFacet().getAnnotation(PartitionKey.class);
    }

//----------------------------------------------------------------------------------------------------------------------
// Constructors
//----------------------------------------------------------------------------------------------------------------------

    public PojoMapping(Class<P> pojoClass, String tableName, List<FacetMapping> facetMappings) {
        this.pojoClass = pojoClass;
        this.tableName = tableName;
        this.idMappings = toIdMappings(facetMappings);
        this.simpleMappings = facetMappings.stream()
                .filter(mapping -> !idMappings.contains(mapping))
                .sorted((left, right) -> left.getFacet().getColumnName().compareTo(right.getFacet().getColumnName()))
                .collect(Collectors.toList());
        if (idMappings.isEmpty()) {
            throw new HecateException("No key fields found for class %s.", pojoClass.getSimpleName());
        }
    }

    private static List<FacetMapping> toIdMappings(List<FacetMapping> allMappings) {
        Optional<FacetMapping> id = annotatedWith(allMappings, Id.class).findFirst();
        if (id.isPresent()) {
            return Collections.singletonList(id.get());
        } else {
            List<FacetMapping> idMappings = new LinkedList<>();
            idMappings.addAll(annotatedWith(allMappings, PartitionKey.class).sorted((left, right) -> partitionKey(left).order() - partitionKey(right).order()).collect(Collectors.toList()));
            idMappings.addAll(annotatedWith(allMappings, ClusteringColumn.class).sorted((left, right) -> clusteringColumn(left).order() - clusteringColumn(right).order()).collect(Collectors.toList()));
            return idMappings;
        }
    }

//----------------------------------------------------------------------------------------------------------------------
// Getter/Setter Methods
//----------------------------------------------------------------------------------------------------------------------

    public Class<P> getPojoClass() {
        return pojoClass;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Create createCreateStatement() {
        Create create = SchemaBuilder.createTable(tableName);
        idMappings.forEach(mapping -> {
            if (mapping.isPartitionKey()) {
                LOGGER.debug("Adding partition key column {}...", mapping.getFacet().getColumnName());
                create.addPartitionKey(mapping.getFacet().getColumnName(), mapping.getColumnType().getDataType());
            } else if (mapping.isClusteringColumn()) {
                LOGGER.debug("Adding clustering column {}...", mapping.getFacet().getColumnName());
                create.addClusteringColumn(mapping.getFacet().getColumnName(), mapping.getColumnType().getDataType());
            }
        });

        simpleMappings.forEach(mapping -> {
            LOGGER.debug("Adding simple column {}...", mapping.getFacet().getColumnName());
            create.addColumn(mapping.getFacet().getColumnName(), mapping.getColumnType().getDataType());
        });
        create.ifNotExists();
        return create;
    }

    public Delete createDeleteStatement() {
        Delete delete = delete().from(tableName);
        idMappings.forEach(mapping -> delete.where(eq(mapping.getFacet().getColumnName(), bindMarker())));
        return delete;
    }

    public Insert createInsertStatement() {
        Insert insert = insertInto(tableName);
        idMappings.forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        simpleMappings.forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        return insert;
    }

    public Select.Where createSelectStatement() {
        Select.Selection select = select();
        idMappings.forEach(mapping -> select.column(mapping.getFacet().getColumnName()));
        simpleMappings.forEach(mapping -> select.column(mapping.getFacet().getColumnName()));
        return select.from(tableName).where();
    }

    public FacetMapping getForeignKeyMapping() {
        if (idMappings.size() == 1) {
            return idMappings.get(0);
        } else {
            throw new HecateException("Class %s contains a composite primary key (%s) and does not support foreign key references.", pojoClass.getSimpleName(), idMappings.stream().map(mapping -> mapping.getFacet().getName()).collect(Collectors.joining(",")));
        }
    }

    public List<FacetMapping> getIdMappings() {
        return Collections.unmodifiableList(idMappings);
    }

    public List<FacetMapping> getSimpleMappings() {
        return Collections.unmodifiableList(simpleMappings);
    }
}
