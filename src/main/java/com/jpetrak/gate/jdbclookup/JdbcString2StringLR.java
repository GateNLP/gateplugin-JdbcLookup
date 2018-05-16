/* 
 * Copyright (C) 2015-2016 The University of Sheffield.
 *
 * This file is part of gateplugin-JdbcLookup
 * (see https://github.com/johann-petrak/gateplugin-JdbcLookup)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software. If not, see <http://www.gnu.org/licenses/>.
 */
package com.jpetrak.gate.jdbclookup;

import gate.Resource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.util.GateRuntimeException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * Language Resource that represents a JDBC table for storing string key/value
 * pairs.
 *
 * CAUTION: this has only ever been tested with H2 and probably does not work
 * out of the box with other JDBC databases!
 *
 * @author Johann Petrak
 */
@CreoleResource(name = "JdbcString2StringLR",
        comment = "A LR representing a table for storing/retrieving String values for String keys")
public class JdbcString2StringLR extends JdbcLR {

  @Optional
  @CreoleParameter(
          comment = "The name of the table to use, if none is specified JdbcString2StringLR will be used",
          defaultValue = "JdbcString2StringLR")
  public void setTableName(String name) {
    tableName = name;
  }

  public String getTableName() {
    return tableName;
  }
  protected String tableName;

  public String getActualTableName() {
    if (tableName == null) {
      return "JdbcString2StringLR";
    } else {
      return tableName;
    }
  }

  @CreoleParameter(
          comment = "If this should be used for reading only",
          defaultValue = "false")
  public void setReadOnly(Boolean flag) {
    readOnly = flag;
  }
  
  public Boolean getReadOnly() {
    return readOnly;
  }
  protected Boolean readOnly = false;

  @Optional
  @CreoleParameter(
          comment = "The name of the table column that contains the key",
          defaultValue = "key")
  public void setKeyColumnName(String name) {
    keyColumnName = name;
  }  
  public String getKeyColumnName() { return keyColumnName; }  
  public String getTheKeyColumnName() { 
    if(keyColumnName==null || keyColumnName.isEmpty()) {
      return "key";
    } else {
      return keyColumnName;
    }
  }
  protected String keyColumnName = "key";
  
  @Optional
  @CreoleParameter(
          comment = "The name of the table column that contains the value",
          defaultValue = "value")
  public void setValueColumnName(String name) {
    valueColumnName = name;
  }  
  public String getValueColumnName() { return valueColumnName; }  
  // NOTE: this is normally just a column escaped in backticks, e.g. `value`
  // but if we detect that the valueColumnName contains parentheses, then 
  // we assume it is a SQL function and omit the backticks
  public String getTheValueColumnName() { 
    if(valueColumnName==null || valueColumnName.isEmpty()) {
      return "value";
    } else {
      if(valueColumnName.contains("(")) {
        return valueColumnName;
      } else {
        return "`"+valueColumnName+"`";
      }
    }
  }
  protected String valueColumnName = "value";
  
  @Override
  public synchronized Resource init() throws ResourceInstantiationException {
    super.init();
    establishTable();
    getSql = getSqlTempl.replaceAll("!!TBL!!", getActualTableName());
    getSql = getSql.replaceAll("!!KEY!!", getTheKeyColumnName());
    getSql = getSql.replaceAll("!!VALUE!!", getTheValueColumnName());
    containsSql = containsSqlTempl.replaceAll("!!TBL!!", getActualTableName());
    containsSql = containsSql.replaceAll("!!KEY!!", getTheKeyColumnName());
    putSql = putSqlTempl.replaceAll("!!TBL!!", getActualTableName());
    putSql = putSql.replaceAll("!!KEY!!", getTheKeyColumnName());
    deleteSql = deleteSqlTempl.replaceAll("!!TBL!!", getActualTableName());
    deleteSql = deleteSql.replaceAll("!!KEY!!", getTheKeyColumnName());
    return this;
  }

  protected void establishTable() {
    PreparedStatement prSt = null;
    // check if we can access the table
    final String selectOne = "SELECT * FROM !!TBL!! LIMIT 1";
    boolean maynothavetable = false;
    Exception theException = null;
    try {
      prSt = connection.prepareStatement(selectOne.replaceAll("!!TBL!!", getActualTableName()));
    } catch (SQLException ex) {
      theException = ex;
      maynothavetable = true;
    }
    try {
      if (!maynothavetable) {
        prSt.execute();
      }
    } catch (SQLException ex) {
      theException = ex;
      maynothavetable = true;
    }
    if (maynothavetable) {
      // if our LR is read-only, this is an error, throw an exception
      if (readOnly) {
        throw new GateRuntimeException("Read only resource and could not access table", theException);
      }
      // try to create the table
      final String createTable = "CREATE TABLE !!TBL!! ( `key` VARCHAR NOT NULL, `value` VARCHAR )";
      final String createIndex = "CREATE UNIQUE INDEX !!IDX!! ON !!TBL!! ( `key`  )";
      try {
        prSt = connection.prepareStatement(createTable.replaceAll("!!TBL!!", getActualTableName()));
      } catch (SQLException ex) {
        shutdownConnection();
        throw new GateRuntimeException("Could not prepare create table statement", ex);
      }
      try {
        prSt.execute();
      } catch (SQLException ex) {
        shutdownConnection();
        throw new GateRuntimeException("Could not execute create table statement", ex);
      }
      try {
        prSt = connection.prepareStatement(createIndex.replaceAll("!!TBL!!", getActualTableName()).replaceAll("!!IDX!!", getActualTableName() + "IndexByKey"));
      } catch (SQLException ex) {
        shutdownConnection();
        throw new GateRuntimeException("Could not prepare create index statement", ex);
      }
      try {
        prSt.execute();
      } catch (SQLException ex) {
        shutdownConnection();
        throw new GateRuntimeException("Could not execute create index statement", ex);
      }
    }
  }

  @Override
  public void cleanup() {
    super.cleanup();
  }

  private void shutdownConnection() {
    try {
      if (connection != null && connection.isValid(1)) {
        connection.close();
      }
    } catch (SQLException ex) {
      throw new GateRuntimeException("Error when disconnecting JDBC connection", ex);
    }
  }

  // API methods
  public Connection getConnection() {
    return connection;
  }
  private static final String getSqlTempl = "SELECT !!VALUE!! FROM !!TBL!! WHERE `!!KEY!!` = ?";
  private static final String containsSqlTempl = "SELECT 1 FROM !!TBL!! WHERE `!!KEY!!` = ? LIMIT 1";
  private static final String putSqlTempl = "MERGE INTO !!TBL!! KEY(`!!KEY!!`) VALUES(?,?)";
  private static final String deleteSqlTempl = "DELETE FROM !!TBL!! WHERE `!!KEY!!` = ?";
  String getSql;

  // returns the value of key. With this call, there is no way to distinguish
  // between a non-existing key or a key that has the value "null" stored, 
  // both return null. 
  public String get(String key) {
    PreparedStatement prSt;
    try {
      prSt = connection.prepareStatement(getSql);
      prSt.setString(1, key);
      ResultSet rs = prSt.executeQuery();
      if (rs.next()) {
        return rs.getString(1);
      } else {
        return null;
      }
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new GateRuntimeException("Could not update Jdbc String2String store", ex);
    }
  }
  // explicitly check if a key is in the key/value store
  String containsSql;

  public boolean contains(String key) {
    PreparedStatement prSt;
    try {
      prSt = connection.prepareStatement(containsSql);
      prSt.setString(1, key);
      ResultSet rs = prSt.executeQuery();
      if (rs.next()) {
        return true;
      } else {
        return false;
      }
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new GateRuntimeException("Could not update Jdbc String2String store", ex);
    }
  }
  String putSql;
// NOTE: at the moment this will always return null. The String return type
// is just here so we may be able to return an "old" value later if we want
// or need to without changing the interface.
  public String put(String key, String value) {
    PreparedStatement prSt;
    if (readOnly) {
      throw new GateRuntimeException("Update not allowed for a read-only String2String store");
    }
    try {
      prSt = connection.prepareStatement(putSql);
      prSt.setString(1, key);
      prSt.setString(2, value);
      prSt.execute();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new GateRuntimeException("Could not update Jdbc String2String store", ex);
    }
    return null;
  }
  String deleteSql;
// we just return null at the moment!
  public String remove(String key) {
    PreparedStatement prSt;
    if (readOnly) {
      throw new GateRuntimeException("Delete not allowed for a read-only String2String store");
    }
    try {
      prSt = connection.prepareStatement(deleteSql);
      prSt.setString(1, key);
      prSt.execute();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new GateRuntimeException("Could not delete from Jdbc String2String store", ex);
    }
    return null;
  }

  public void removeAll(Iterable<String> keys) {
    Iterator<String> it = keys.iterator();
    while(it.hasNext()) {
      remove(it.next());
    }
  }
  
}
