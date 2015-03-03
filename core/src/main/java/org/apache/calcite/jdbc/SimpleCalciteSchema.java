/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.jdbc;

import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.Compatible;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * An {@link org.apache.calcite.jdbc.CalciteSchema} implementation that
 * maintains minimal state.
 */
public class SimpleCalciteSchema extends CalciteAbstractSchema {

  private Map<String, SimpleCalciteSchema> subSchemaMap = Maps.newHashMap();
  private Map<String, TableEntry> tableMap = Maps.newHashMap();

  public SimpleCalciteSchema(CalciteSchema parent, Schema schema, String name) {
    super(parent, schema, name);
  }

  @Override
  public TableEntry add(String tableName, Table table) {
    TableEntry e = new TableEntryImpl(this, tableName, table,
        ImmutableList.<String>of());
    tableMap.put(tableName, e);
    return e;
  }

  @Override
  public TableEntry add(String tableName,
                        Table table,
                        ImmutableList<String> sqls) {
    final TableEntryImpl entry =
        new TableEntryImpl(this, tableName, table, sqls);
    tableMap.put(tableName, entry);
    return entry;
  }

  @Override
  public TableEntry getTableBySql(String sql) {
    for (TableEntry tableEntry : tableMap.values()) {
      if (tableEntry.sqls.contains(sql)) {
        return tableEntry;
      }
    }

    return null;
  }

  @Override
  public TableEntry getTable(String tableName, boolean caseSensitive) {
    Table t = schema.getTable(tableName);
    if (t == null) {
      TableEntry e = tableMap.get(tableName);
      if (e != null) {
        t = e.getTable();
      }
    }
    if (t != null) {
      return new TableEntryImpl(this, tableName, t, ImmutableList.<String>of());
    }

    return null;
  }

  @Override
  public NavigableSet<String> getTableNames() {
    return Compatible.INSTANCE.navigableSet(
        ImmutableSortedSet.copyOf(
            Sets.union(schema.getTableNames(), tableMap.keySet())));
  }

  @Override
  public CalciteSchema add(String name, Schema schema) {
    SimpleCalciteSchema s = new SimpleCalciteSchema(this, schema, name);
    subSchemaMap.put(name, s);
    return s;
  }

  @Override
  public CalciteSchema getSubSchema(String schemaName, boolean caseSensitive) {
    Schema s = schema.getSubSchema(schemaName);
    if (s != null) {
      return new SimpleCalciteSchema(this, s, schemaName);
    }
    return subSchemaMap.get(schemaName);
  }

  @Override
  public NavigableMap<String, CalciteSchema> getSubSchemaMap() {
    return Compatible.INSTANCE.navigableMap(
        ImmutableSortedMap.<String, CalciteSchema>copyOf(subSchemaMap));
  }

  @Override
  public FunctionEntry add(String name, Function function) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support add function yet!");
  }

  @Override
  public Collection<Function> getFunctions(String name, boolean caseSensitive) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public NavigableSet<String> getFunctionNames() {
    return Compatible.INSTANCE.navigableSet(ImmutableSortedSet.<String>of());
  }

  @Override
  public NavigableMap<String, Table> getTablesBasedOnNullaryFunctions() {
    return Compatible.INSTANCE.navigableMap(
        ImmutableSortedMap.<String, Table>of());
  }

  @Override
  // TODO
  public TableEntry getTableBasedOnNullaryFunction(String tableName,
                                                   boolean caseSensitive) {
    return null;
  }

  @Override
  public LatticeEntry add(String name, Lattice lattice) {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support add lattice yet!");
  }

  @Override
  public NavigableMap<String, LatticeEntry> getLatticeMap() {
    throw new UnsupportedOperationException(
        this.getClass().getSimpleName() + " does not support getLatticeMap yet!");
  }

  @Override
  public void setCache(boolean cache) {
    throw new UnsupportedOperationException();
  }


  @Override
  public boolean isCacheEnabled() {
    return false;
  }

  public static SchemaPlus createRootSchema(boolean addMetadataSchema) {
    SimpleCalciteRootSchema rootSchema =
        new SimpleCalciteRootSchema(new CalciteConnectionImpl.RootSchema());
    if (addMetadataSchema) {
      rootSchema.add("metadata", MetadataSchema.INSTANCE);
    }
    return rootSchema.plus();
  }
}
