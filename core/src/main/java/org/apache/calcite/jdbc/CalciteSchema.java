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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Define interface to wrap around user-defined schema used internally
 */
public interface CalciteSchema {

  /**
   * @return parent schema.
   */
  CalciteSchema getParent();

  /**
   *
   * @return root schema.
   */
  CalciteRootSchema root();

  /**
   * @return schema name.
   */
  String getName();

  /**
   * @return SchemaPlus, which provides extra functionality than Schema.
   */
  SchemaPlus plus();

  /**
   * @return schema that is wrapped by CalciteSchema.
   */
  Schema getSchema();

  /** Returns the default path resolving functions from this schema.
   *
   * <p>The path consists is a list of lists of strings.
   * Each list of strings represents the path of a schema from the root schema.
   * For example, [[], [foo], [foo, bar, baz]] represents three schemas: the
   * root schema "/" (level 0), "/foo" (level 1) and "/foo/bar/baz" (level 3).
   *
   * @return Path of this schema; never null, may be empty
   */
  List<? extends List<String>> getPath();

  /** Returns the path of an object in this schema. */
  List<String> path(String name);

  /** Defines a table within this schema. */
  TableEntry add(String tableName, Table table, ImmutableList<String> sqls);

  /** Defines a table within this schema. */
  TableEntry add(String tableName, Table table);

  /** Adds a child schema of this schema. */
  CalciteSchema add(String name, Schema schema);

  /** Add a function to this schema */
  FunctionEntry add(String name, Function function);

  /** Add a lattice to this schema */
  LatticeEntry add(String name, Lattice lattice);

  /** Returns a table that materializes the given SQL statement. */
  TableEntry getTableBySql(String sql);

  /** Returns a table with the given name. Does not look for views. */
  TableEntry getTable(String tableName, boolean caseSensitive);

  /** Returns a CalciteSchema matching the provided name */
  CalciteSchema getSubSchema(String schemaName, boolean caseSensitive);

  /** Returns a collection of sub-schemas, both explicit (defined using
   * {@link #add(String, org.apache.calcite.schema.Schema)}) and implicit
   * (defined using {@link org.apache.calcite.schema.Schema#getSubSchemaNames()}
   * and {@link Schema#getSubSchema(String)}). */
  NavigableMap<String, CalciteSchema> getSubSchemaMap();

  /** Returns a collection of lattices.
   *
   * <p>All are explicit (defined using {@link #add(String, Lattice)}). */
  NavigableMap<String, LatticeEntry> getLatticeMap();

  /** Returns the set of all table names. Includes implicit and explicit tables
   * and functions with zero parameters. */
  NavigableSet<String> getTableNames();

  /** Returns a collection of all functions, explicit and implicit, with a given
   * name. Never null. */
  Collection<Function> getFunctions(String name, boolean caseSensitive);

  /** Returns the list of function names in this schema, both implicit and
   * explicit, never null. */
  NavigableSet<String> getFunctionNames();

  /** Returns tables derived from explicit and implicit functions
   * that take zero parameters. */
  NavigableMap<String, Table> getTablesBasedOnNullaryFunctions();

  /** Returns a tables derived from explicit and implicit functions
   * that take zero parameters. */
  TableEntry getTableBasedOnNullaryFunction(String tableName,
                                            boolean caseSensitive);

  /** turn on/off cache */
  void setCache(boolean cache);

  /** Check if cache is enabled */
  boolean isCacheEnabled();

  /**
   *
   */
  public abstract static class Entry {
    public final CalciteSchema schema;
    public final String name;

    public Entry(CalciteSchema schema, String name) {
      this.schema = Preconditions.checkNotNull(schema);
      this.name = Preconditions.checkNotNull(name);
    }

    /** Returns this object's path. For example ["hr", "emps"]. */
    public final List<String> path() {
      return schema.path(name);
    }
  }

  /** Membership of a table in a schema. */
  public abstract static class TableEntry extends Entry {
    public final List<String> sqls;

    public TableEntry(CalciteSchema schema, String name,
                      ImmutableList<String> sqls) {
      super(schema, name);
      this.sqls = Preconditions.checkNotNull(sqls);
    }

    public abstract Table getTable();
  }

  /** Membership of a function in a schema. */
  public abstract static class FunctionEntry extends Entry {
    public FunctionEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract Function getFunction();

    /** Whether this represents a materialized view. (At a given point in time,
     * it may or may not be materialized as a table.) */
    public abstract boolean isMaterialization();
  }

  /** Membership of a lattice in a schema. */
  public abstract static class LatticeEntry extends Entry {
    public LatticeEntry(CalciteSchema schema, String name) {
      super(schema, name);
    }

    public abstract Lattice getLattice();

    public abstract TableEntry getStarTable();
  }

}
