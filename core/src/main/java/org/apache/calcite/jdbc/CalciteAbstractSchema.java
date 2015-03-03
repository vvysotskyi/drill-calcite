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

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.schema.impl.StarTable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Abstract implementation of {@link org.apache.calcite.jdbc.CalciteSchema}.
 */
public abstract class CalciteAbstractSchema implements CalciteSchema {
  protected final CalciteSchema parent;
  protected final Schema schema;
  protected final String name;
  private ImmutableList<ImmutableList<String>> path;

  public CalciteAbstractSchema(CalciteSchema parent, final Schema schema, String name) {
    this.parent = parent;
    this.schema = schema;
    this.name = name;
  }

  @Override
  public CalciteSchema getParent() {
    return parent;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public String getName() {
    return name;
  }

  /** Defines a table within this schema. */
  @Override
  public TableEntry add(String tableName, Table table) {
    return add(tableName, table, ImmutableList.<String>of());
  }


  @Override
  public CalciteRootSchema root() {
    for (CalciteSchema schema = this;;) {
      if (schema.getParent() == null) {
        return (CalciteRootSchema) schema;
      }
      schema = schema.getParent();
    }
  }

  /** Returns the path of an object in this schema. */
  @Override
  public List<String> path(String name) {
    final List<String> list = new ArrayList<String>();
    if (name != null) {
      list.add(name);
    }
    for (CalciteSchema s = this; s != null; s = s.getParent()) {
      if (s.getParent() != null || !s.getName().equals("")) {
        // Omit the root schema's name from the path if it's the empty string,
        // which it usually is.
        list.add(s.getName());
      }
    }
    return ImmutableList.copyOf(Lists.reverse(list));
  }

  @Override
  public SchemaPlus plus() {
    return new SchemaPlusImpl();
  }

  /** Returns the default path resolving functions from this schema.
   *
   * <p>The path consists is a list of lists of strings.
   * Each list of strings represents the path of a schema from the root schema.
   * For example, [[], [foo], [foo, bar, baz]] represents three schemas: the
   * root schema "/" (level 0), "/foo" (level 1) and "/foo/bar/baz" (level 3).
   *
   * @return Path of this schema; never null, may be empty
   */
  @Override
  public List<? extends List<String>> getPath() {
    if (path != null) {
      return path;
    }
    // Return a path consisting of just this schema.
    return ImmutableList.of(path(null));
  }


  public static CalciteSchema from(SchemaPlus plus) {
    return ((SchemaPlusImpl) plus).calciteSchema();
  }

  /**
   * Entry in a schema, such as a table or sub-schema.
   *
   * <p>Each object's name is a property of its membership in a schema;
   * therefore in principle it could belong to several schemas, or
   * even the same schema several times, with different names. In this
   * respect, it is like an inode in a Unix file system.</p>
   *
   * <p>The members of a schema must have unique names.
   */

  /** Implementation of {@link SchemaPlus} based on a
   * {@link org.apache.calcite.jdbc.CalciteSchema}. */
  private class SchemaPlusImpl implements SchemaPlus {
    CalciteSchema calciteSchema() {
      return CalciteAbstractSchema.this;
    }

    public SchemaPlus getParentSchema() {
      return parent == null ? null : parent.plus();
    }

    public String getName() {
      return CalciteAbstractSchema.this.getName();
    }

    public boolean isMutable() {
      return schema.isMutable();
    }

    public void setCacheEnabled(boolean cache) {
      CalciteAbstractSchema.this.setCache(cache);
    }

    public boolean isCacheEnabled() {
      return CalciteAbstractSchema.this.isCacheEnabled();
    }

    public boolean contentsHaveChangedSince(long lastCheck, long now) {
      return schema.contentsHaveChangedSince(lastCheck, now);
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
      return schema.getExpression(parentSchema, name);
    }

    public Table getTable(String name) {
      final TableEntry entry = CalciteAbstractSchema.this.getTable(name, true);
      return entry == null ? null : entry.getTable();
    }

    public NavigableSet<String> getTableNames() {
      return CalciteAbstractSchema.this.getTableNames();
    }

    public Collection<Function> getFunctions(String name) {
      return CalciteAbstractSchema.this.getFunctions(name, true);
    }

    public NavigableSet<String> getFunctionNames() {
      return CalciteAbstractSchema.this.getFunctionNames();
    }

    public SchemaPlus getSubSchema(String name) {
      final CalciteSchema subSchema =
          CalciteAbstractSchema.this.getSubSchema(name, true);
      return subSchema == null ? null : subSchema.plus();
    }

    public Set<String> getSubSchemaNames() {
      return CalciteAbstractSchema.this.getSubSchemaMap().keySet();
    }

    public SchemaPlus add(String name, Schema schema) {
      final CalciteSchema calciteSchema = CalciteAbstractSchema.this.add(name, schema);
      return calciteSchema.plus();
    }

    public <T> T unwrap(Class<T> clazz) {
      if (clazz.isInstance(this)) {
        return clazz.cast(this);
      }
      if (clazz.isInstance(CalciteAbstractSchema.this)) {
        return clazz.cast(CalciteAbstractSchema.this);
      }
      if (clazz.isInstance(CalciteAbstractSchema.this.schema)) {
        return clazz.cast(CalciteAbstractSchema.this.schema);
      }
      throw new ClassCastException("not a " + clazz);
    }

    public void setPath(ImmutableList<ImmutableList<String>> path) {
      CalciteAbstractSchema.this.path = path;
    }

    public void add(String name, Table table) {
      CalciteAbstractSchema.this.add(name, table);
    }

    public void add(String name, Function function) {
      CalciteAbstractSchema.this.add(name, function);
    }

    public void add(String name, Lattice lattice) {
      CalciteAbstractSchema.this.add(name, lattice);
    }
  }

  /**
   * Implementation of {@link CalciteSchema.TableEntry}
   * where all properties are held in fields.
   */
  public static class TableEntryImpl extends TableEntry {
    private final Table table;

    /** Creates a TableEntryImpl. */
    public TableEntryImpl(CalciteSchema schema, String name, Table table,
        ImmutableList<String> sqls) {
      super(schema, name, sqls);
      assert table != null;
      this.table = Preconditions.checkNotNull(table);
    }

    public Table getTable() {
      return table;
    }
  }

  /**
   * Implementation of {@link FunctionEntry}
   * where all properties are held in fields.
   */
  public static class FunctionEntryImpl extends FunctionEntry {
    private final Function function;

    /** Creates a FunctionEntryImpl. */
    public FunctionEntryImpl(CalciteSchema schema, String name,
        Function function) {
      super(schema, name);
      this.function = function;
    }

    public Function getFunction() {
      return function;
    }

    public boolean isMaterialization() {
      return function
          instanceof MaterializedViewTable.MaterializedViewTableMacro;
    }
  }

  /**
   * Implementation of {@link LatticeEntry}
   * where all properties are held in fields.
   */
  public static class LatticeEntryImpl extends LatticeEntry {
    private final Lattice lattice;
    private final CalciteSchema.TableEntry starTableEntry;

    /** Creates a LatticeEntryImpl. */
    public LatticeEntryImpl(CalciteSchema schema, String name,
        Lattice lattice) {
      super(schema, name);
      this.lattice = lattice;

      // Star table has same name as lattice and is in same schema.
      final StarTable starTable = lattice.createStarTable();
      starTableEntry = schema.add(name, starTable);
    }

    public Lattice getLattice() {
      return lattice;
    }

    public TableEntry getStarTable() {
      return starTableEntry;
    }
  }

}

// End CalciteAbstractSchema.java
