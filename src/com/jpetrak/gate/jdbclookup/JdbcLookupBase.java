package com.jpetrak.gate.jdbclookup;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Controller;
import gate.Document;
import gate.Resource;
import gate.creole.AbstractLanguageAnalyser;
import gate.creole.ControllerAwarePR;
import gate.creole.ExecutionException;
import gate.creole.ExecutionInterruptedException;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.Optional;
import gate.creole.metadata.RunTime;
import gate.util.GateRuntimeException;
import java.io.File;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

// TODO: only re-prepare the statement if the effective SQL would have changed
// from what we already have!
// To do this, remember the last effective SQL and compare to what we would 
// get now and only re-prepare/re-remember if different!


/**
 * Common code for both the JdbcLookup and JdbcJsonLookup PRs
 * 
 * @author Johann Petrak
 */
public class JdbcLookupBase 
  extends AbstractLanguageAnalyser 
  implements ControllerAwarePR
{

  protected String inputASName = "";
  @RunTime
  @Optional
  @CreoleParameter(
          comment = "Input annotation set",
          defaultValue = "")
  public void setInputAnnotationSet(String ias) {
    inputASName = ias;
  }

  public String getInputAnnotationSet() {
    return inputASName;
  }
  protected String inputType = "";

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The input annotation type",
          defaultValue = "Lookup")
  public void setInputAnnotationType(String val) {
    this.inputType = val;
  }

  public String getInputAnnotationType() {
    return inputType;
  }

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The optional containing annotation set type",
          defaultValue = "")
  public void setContainingAnnotationType(String val) {
    this.containingType = val;
  }

  public String getContainingAnnotationType() {
    return containingType;
  }
  protected String containingType = "";

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The feature from the input annotation to use as key, if left blank the document text",
          defaultValue = "")
  public void setKeyFeature(String val) {
    this.keyFeature = val;
  }

  public String getKeyFeature() {
    return keyFeature;
  }
  protected String keyFeature = "";
  
  
  // Output 
  protected String outputASName = "";

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "Output annotation set",
          defaultValue = "")
  public void setOutputAnnotationSet(String ias) {
    outputASName = ias;
  }

  public String getOutputAnnotationSet() {
    return outputASName;
  }
  protected String outputType = "";

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The output annotation type",
          defaultValue = "LookupData")
  public void setOutputAnnotationType(String val) {
    this.outputType = val;
  }

  public String getOutputAnnotationType() {
    return outputType;
  }
  
  protected String listType = "";

  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The annotation type of the list annotations, if empty, none will be created",
          defaultValue = "LookupList")
  public void setListAnnotationType(String val) {
    this.listType = val;
  }

  public String getListAnnotationType() {
    return listType;
  }
  
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
          comment = "The JDBC URL, may contain $prop{name} or $env{name} or ${relpath}",
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
  {
    try {
      dbDirectoryUrl = new URL("file://.");
    } catch(Exception ex) {
      //
    }
  }
  @Optional
  @CreoleParameter(
          comment = "The location of where a file database is stored. This is not used directly but can be used to replace the ${dbdirectory} variable in the jdbcUrl parameter", defaultValue="file://.")
  public void setDbDirectoryUrl(URL dir) {
    dbDirectoryUrl = dir;
  }
  public URL getDbDirectoryUrl() {
    return dbDirectoryUrl;
  }
  
  protected String sqlQuery = "SELECT <<jsonfieldname>> FROM <<tablename>> WHERE <<keyfieldname>> = ?";
  @RunTime
  @CreoleParameter(
          comment = "The SQL to use for getting the json field for a key",
          defaultValue = "SELECT <<jsonfieldname>> FROM <<tablename>> WHERE <<keyfieldname>> = ?")
  public void setSqlQuery(String q) {
    sqlQuery = q;
  }
  public String getSqlQuery() {
    return sqlQuery;
  }
  
  
  protected Logger logger;
  protected PreparedStatement stSelect;
  protected Connection connection;

  @Override
  public synchronized Resource init() throws ResourceInstantiationException {
    super.init();
    if(getJdbcDriver() == null) {
      throw new ResourceInstantiationException("jdbcDriver must be specified");
    }
    if(getSqlQuery()== null) {
      throw new ResourceInstantiationException("SQL query must be specified");
    }
    // TODO: check all other init parms
    establishConnection();
    return this;
  }
  
  private void establishConnection() {
    try {
      Class.forName(jdbcDriver);
    } catch (ClassNotFoundException ex) {
      throw new GateRuntimeException("Could not load JDBC driver " + jdbcDriver, ex);
    }
    try {
      // expand any variables in the url
      // First we have to create a map and put the relpath in there so it
      // can get replaced too
      //
      System.out.println("Relpath URL is: "+getDbDirectoryUrl());
      String dbdirectory = "";
      if(getDbDirectoryUrl().getProtocol().equals("file")) {
        dbdirectory = getDbDirectoryUrl().getPath();
        dbdirectory = new File(dbdirectory).getAbsolutePath();
      } else {
        throw new GateRuntimeException("The database directory URL is not a file URL");
      }
      Map<String,String> dbdirectoryMap = new HashMap<String,String>();
      dbdirectoryMap.put("dbdirectory", dbdirectory);
      
      String expandedUrl = 
        gate.Utils.replaceVariablesInString(jdbcUrl, dbdirectoryMap, this);
      String expandedUser = 
        gate.Utils.replaceVariablesInString(jdbcUser, dbdirectoryMap, this);
      String expandedPassword = 
        gate.Utils.replaceVariablesInString(jdbcPassword, dbdirectoryMap, this);
      
      System.out.println("Using JDBC URL: "+expandedUrl);
      connection = DriverManager.getConnection(expandedUrl, expandedUser, expandedPassword);
    } catch (SQLException ex) {
      throw new GateRuntimeException("Could not establish JDBC connection",ex);
    }
  }
  
  protected void prepareStatement(Controller controller) {
    String sql = getSqlQuery();
    sql = gate.Utils.replaceVariablesInString(sql, this, controller);
    try {
      // System.out.println("Final SQL used is "+sql);
      stSelect = connection.prepareStatement(sql);      
    } catch (SQLException ex) {
      throw new GateRuntimeException("Could not prepare query statement:\n"+sql,ex);
    }
  }
  
  
  @Override
  public void cleanup() {
    shutdownConnection();
  }

  @Override
  public void reInit() throws ResourceInstantiationException {
    shutdownConnection();
    init();
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
 
  @Override
  public void execute() throws ExecutionException {
    doExecute(document); // delegate so that a subclass can overwrite execute() and still use doExecute
  }

  public void doExecute(Document theDocument) throws ExecutionException {
    interrupted = false;
    if (theDocument == null) {
      throw new ExecutionException("No document to process!");
    }

    AnnotationSet inputAS = null;
    if (inputASName == null
            || inputASName.isEmpty()) {
      inputAS = theDocument.getAnnotations();
    } else {
      inputAS = theDocument.getAnnotations(inputASName);
    }

    AnnotationSet inputAnns = null;
    if (inputType == null || inputType.isEmpty()) {
      throw new GateRuntimeException("Input annotation type must not be empty!");
    }
    inputAnns = inputAS.get(inputType);

    AnnotationSet containingAnns = null;
    if (containingType == null || containingType.isEmpty()) {
      // leave the containingAnns null to indicate we do not use containing annotations
    } else {
      containingAnns = inputAS.get(containingType);
      //System.out.println("DEBUG: got containing annots: "+containingAnns.size()+" type is "+containingAnnotationType);
    }

    AnnotationSet outputAS = document.getAnnotations(outputASName);

    fireStatusChanged("BdbJsonLookup: performing look-up in " + theDocument.getName() + "...");

    if (containingAnns == null) {
      // go through all input annotations 
      for (Annotation ann : inputAnns) {
        doLookup(theDocument, ann, outputAS);
        if(isInterrupted()) {
          throw new ExecutionInterruptedException("JdbcJsonLookup has been interrupted");
        }
      }
    } else {
      // go through the input annotations contained in the contianing annotations
      for (Annotation containingAnn : containingAnns) {
        AnnotationSet containedAnns = gate.Utils.getContainedAnnotations(inputAnns, containingAnn);
        for (Annotation ann : containedAnns) {
          doLookup(theDocument, ann, outputAS);
          if(isInterrupted()) { 
            throw new ExecutionInterruptedException("JdbcJsonLookup has been interrupted");
          }
        }
      }
    }

    fireProcessFinished();
    fireStatusChanged("BdbJsonLookup: look-up complete!");

  }

  public void doLookup(Document doc, Annotation ann, AnnotationSet outputAS) {  
    throw new GateRuntimeException("Must not invoked JdbcLookupBase.doLookup directly");
  }

  @Override
  public void controllerExecutionStarted(Controller cntrlr) throws ExecutionException {
    prepareStatement(cntrlr);
  }

  @Override
  public void controllerExecutionFinished(Controller cntrlr) throws ExecutionException {
    // nothing to do here
  }

  @Override
  public void controllerExecutionAborted(Controller cntrlr, Throwable thrwbl) throws ExecutionException {
    // nothing to do here
  }
  
}
