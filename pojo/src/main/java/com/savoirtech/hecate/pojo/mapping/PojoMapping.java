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
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.savoirtech.hecate.annotation.Cascade;
import com.savoirtech.hecate.annotation.ClusteringColumn;
import com.savoirtech.hecate.annotation.Id;
import com.savoirtech.hecate.annotation.PartitionKey;
import com.savoirtech.hecate.core.exception.HecateException;
import com.savoirtech.hecate.pojo.mapping.facet.FacetMapping;
import com.savoirtech.hecate.pojo.mapping.facet.FacetMappingVisitor;
import com.savoirtech.hecate.pojo.mapping.facet.ScalarFacetMapping;
import com.savoirtech.hecate.pojo.util.PojoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private final List<ScalarFacetMapping> idMappings;
    private final List<FacetMapping> simpleMappings;
    private final int ttl;
    private final boolean cascadeDelete;
    private final boolean cascadeSave;

//----------------------------------------------------------------------------------------------------------------------
// Static Methods
//----------------------------------------------------------------------------------------------------------------------

    private static <A extends Annotation,M extends FacetMapping> Stream<M> annotatedWith(List<M> mappings, Class<A> annotationType) {
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

    public PojoMapping(Class<P> pojoClass, String tableName, List<ScalarFacetMapping> idMappings, List<FacetMapping> simpleMappings) {
        this.pojoClass = pojoClass;
        this.tableName = tableName;
        this.idMappings = sorted(idMappings);
        this.simpleMappings = simpleMappings;
        this.ttl = PojoUtils.getTtl(pojoClass);
        this.cascadeSave = simpleMappings.stream().filter(FacetMapping::isReference).filter(mapping -> !mapping.getFacet().hasAnnotation(Cascade.class) || mapping.getFacet().getAnnotation(Cascade.class).save()).findFirst().isPresent();
        this.cascadeDelete = simpleMappings.stream().filter(FacetMapping::isReference).filter(mapping -> !mapping.getFacet().hasAnnotation(Cascade.class) || mapping.getFacet().getAnnotation(Cascade.class).delete()).findFirst().isPresent();
    }

    private static List<ScalarFacetMapping> sorted(List<ScalarFacetMapping> idMappings) {
        if(idMappings.size() > 1) {
            List<ScalarFacetMapping> sorted = new ArrayList<>(idMappings.size());
            sorted.addAll(annotatedWith(idMappings,PartitionKey.class).sorted((left,right) -> partitionKey(left).order() - partitionKey(right).order()).collect(Collectors.toList()));
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

    public boolean isCascadeSave() {
        return cascadeSave;
    }

//----------------------------------------------------------------------------------------------------------------------
// Canonical Methods
//----------------------------------------------------------------------------------------------------------------------

    public String toString() {
        return pojoClass.getCanonicalName() + "@" + tableName;
    }

//----------------------------------------------------------------------------------------------------------------------
// Other Methods
//----------------------------------------------------------------------------------------------------------------------

    public Create createCreateStatement() {
        Create create = SchemaBuilder.createTable(tableName);
        idMappings.forEach(mapping -> {
            if (mapping.getFacet().hasAnnotation(PartitionKey.class) || mapping.getFacet().hasAnnotation(Id.class)) {
                LOGGER.debug("Adding partition key column {}...", mapping.getFacet().getColumnName());
                create.addPartitionKey(mapping.getFacet().getColumnName(), mapping.getDataType());
            } else if (mapping.getFacet().hasAnnotation(ClusteringColumn.class)) {
                LOGGER.debug("Adding clustering column {}...", mapping.getFacet().getColumnName());
                create.addClusteringColumn(mapping.getFacet().getColumnName(), mapping.getDataType());
            }
        });

        simpleMappings.forEach(mapping -> {
            LOGGER.debug("Adding simple column {}...", mapping.getFacet().getColumnName());
            create.addColumn(mapping.getFacet().getColumnName(), mapping.getDataType());
        });
        create.ifNotExists();
        return create;
    }
    
    public Delete createDeleteStatement() {
        Delete delete = delete().from(tableName);
        idMappings.forEach(mapping -> delete.where(in(mapping.getFacet().getColumnName(), bindMarker())));
        return delete;
    }

    public Select.Where createFindForDeleteStatement() {
        if(!isCascadeDelete()) {
            throw new HecateException("POJO class %s does not support cascaded deletes.", getPojoClass().getCanonicalName());
        }
        Select.Selection select = select();
        simpleMappings.stream().filter(FacetMapping::isReference).forEach(mapping -> select.column(mapping.getFacet().getColumnName()));
        return select.from(tableName).where(in(getForeignKeyMapping().getFacet().getColumnName(),bindMarker()));
    }

    public Insert createInsertStatement() {
        Insert insert = insertInto(tableName);
        idMappings.forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        simpleMappings.forEach(mapping -> insert.value(mapping.getFacet().getColumnName(), bindMarker()));
        insert.using(QueryBuilder.ttl(QueryBuilder.bindMarker()));
        return insert;
    }

    public Select.Where createSelectStatement() {
        Select.Selection select = select();
        idMappings.forEach(mapping -> select.column(mapping.getFacet().getColumnName()));
        simpleMappings.forEach(mapping -> select.column(mapping.getFacet().getColumnName()));
        return select.from(tableName).where();
    }

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

    public void visitFacetMappings(FacetMappingVisitor visitor) {

    }
}
