/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jpetrak.gate.jdbclookup;

import gate.Resource;
import gate.creole.AbstractLanguageResource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.Optional;
import gate.util.GateRuntimeException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author johann
 */
@CreoleResource(name = "JdbcLR",
        comment = "A LR representing a JDBC connection")
public class JdbcLR extends AbstractLanguageResource {
  // JDBC settings
  // The JDBC URL will replace something like $prop{name} with a property name
  // and something like $env{name} with the environment variable.
  protected String jdbcDriver = "org.h2.Driver";

  @CreoleParameter(
          comment = "The JDBC driver to use",
          defaultValue = "org.h2.Driver")
  public void setJdbcDriver(String driver) {
    jdbcDriver = driver;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }
  protected String jdbcUrl = "";

  @CreoleParameter(
          comment = "The JDBC URL, may contain $prop{name} or $env{name} or ${dbdirectory}",
          defaultValue = "jdbc:h2:${dbdirectory}/YOURDBPREFIX")
  public void setJdbcUrl(String url) {
    jdbcUrl = url;
  }

  public String getJdbcUrl() {
    return jdbcUrl;
  }
  protected String jdbcUser = "";

  @Optional
  @CreoleParameter(
          comment = "The JDBC user id",
          defaultValue = "")
  public void setJdbcUser(String user) {
    jdbcUser = user;
  }

  public String getJdbcUser() {
    return jdbcUser;
  }
  protected String jdbcPassword = "";

  @Optional
  @CreoleParameter(
          comment = "The JDBC password",
          defaultValue = "")
  public void setJdbcPassword(String pw) {
    jdbcPassword = pw;
  }
  public String getJdbcPassword() {
    return jdbcPassword;
  }
  
  protected URL dbDirectoryUrl = null;

  @Optional
  @CreoleParameter(
          comment = "The location of where a file database is stored. This is not used directly but can be used to replace the ${dbdirectory} variable in the jdbcUrl parameter")
  public void setDbDirectoryUrl(URL dir) {
    dbDirectoryUrl = dir;
  }
  public URL getDbDirectoryUrl() {
    return dbDirectoryUrl;
  }
  
  protected Connection connection;

  @Override
  public synchronized Resource init() throws ResourceInstantiationException {
    super.init();
    if(getJdbcDriver() == null) {
      throw new ResourceInstantiationException("jdbcDriver must be specified");
    }
    establishConnection();
    return this;
  }
  
  protected void establishConnection() {
    try {
      Class.forName(jdbcDriver);
    } catch (ClassNotFoundException ex) {
      throw new GateRuntimeException("Could not load JDBC driver " + jdbcDriver, ex);
    }
    try {
      // expand any variables in the url
      // First we have to create a map and put the dbdirectory path in 
      //
      System.out.println("Relpath URL is: "+getDbDirectoryUrl());
      try {
        System.out.println("Relpath canonical file is: "+gate.util.Files.fileFromURL(getDbDirectoryUrl()).getCanonicalPath());
      } catch (IOException ex) {
        System.out.println("Could not get canonical path: "+ex);
      }
      String dbdirectory = "";
      if(getDbDirectoryUrl().getProtocol().equals("file")) {
        dbdirectory = getDbDirectoryUrl().getPath();
        dbdirectory = new File(dbdirectory).getAbsolutePath();
      } else {
        throw new GateRuntimeException("The database directory URL is not a file URL");
      }
      Map<String,String> dbdirectoryMap = new HashMap<String,String>();
      dbdirectoryMap.put("dbdirectory", dbdirectory);
      
      expandedUrlString = 
        gate.Utils.replaceVariablesInString(jdbcUrl, dbdirectoryMap, this);
      String expandedUser = 
        gate.Utils.replaceVariablesInString(jdbcUser, dbdirectoryMap, this);
      String expandedPassword = 
        gate.Utils.replaceVariablesInString(jdbcPassword, dbdirectoryMap, this);
      
      System.out.println("Using JDBC URL: "+expandedUrlString);
      connection = DriverManager.getConnection(expandedUrlString, expandedUser, expandedPassword);
    } catch (SQLException ex) {
      throw new GateRuntimeException("Could not establish JDBC connection",ex);
    }
  }
  
  protected String expandedUrlString;
  
  @Override
  public void cleanup() {
    shutdownConnection();
  }

  private void shutdownConnection() {
    try {
      if(connection != null && connection.isValid(1)) {
        connection.close();
      }
    } catch (SQLException ex) {
      throw new GateRuntimeException("Error when disconnecting JDBC connection",ex);
    }
  }
 
  // API methods
  
  public Connection getConnection() { return connection; }
  public String getExpandedUrlString() { return expandedUrlString; }
  
}
