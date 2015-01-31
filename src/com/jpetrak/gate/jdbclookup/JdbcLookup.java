/*
 *  JdbcLookup.java
 *
 */

// NOTE: this needs GATE revision >= 17566 to work since it uses a gate.Utils
// method that was added then.

// TODO: catch any exception we throw in execute() and attempt to close the
// connection properly so we do not leak connections in case of an error

package com.jpetrak.gate.jdbclookup;


import gate.*;
import gate.creole.*;
import gate.creole.metadata.*;
import gate.util.GateRuntimeException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


@CreoleResource(name = "JdbcLookup",
        comment = "Lookup strings in a database and add annotations for each result row")
public class JdbcLookup  extends JdbcLookupBase {

  protected ProcessingMode processingMode = ProcessingMode.AddFeaturesFromFirst;

  @RunTime
  @CreoleParameter(
          comment = "Processing mode",
          defaultValue = "AddFeaturesFromFirst")
  public void setProcessingMode(ProcessingMode mode) {
    processingMode = mode;
  }
  public ProcessingMode getProcessingMode() {
    return processingMode;
  }

  
  protected FeatureMap nameMappings = Factory.newFeatureMap();
  @RunTime
  @Optional
  @CreoleParameter(comment="Map from SQL column names to feature names")
  public void setNameMappings(FeatureMap mappings) {
    nameMappings = mappings;
  }
  public FeatureMap getNameMappings() {
    return nameMappings;
  }
  
  
  @Override
  public Resource init() throws ResourceInstantiationException {
    super.init();
    return this;
  }
  
  private ResultSetMetaData rsmd = null;
  private List<String> columnNames = null;
  private List<Object> columnValues = null;
  private int columnCount = 0;
   
  @Override
  public void doLookup(Document doc, Annotation ann, AnnotationSet outputAS) {
    //System.out.println("doLookup for "+ann);
    FeatureMap fm = ann.getFeatures();
    String key = "";
    if (keyFeature.isEmpty()) {
      key = gate.Utils.stringFor(doc, ann);
    } else {
      Object val = fm.get(keyFeature);
      if (val != null) {
        key = val.toString();
      }
    }
    // Now we should have our key ... if it is not empty, lets use it
    if (!key.isEmpty()) {
      // query the db and create the result set
      // go through all the results in the result set and act according to
      // the processing mode
      try {
        //System.out.println("Lookup up "+key);
        stSelect.setString(1,key);
      } catch (SQLException ex) {
        throw new GateRuntimeException("Could not set query parameter to '"+key+"'",ex);
      }
      ResultSet rs = null;
      try {
        rs = stSelect.executeQuery();
      } catch (SQLException ex) {
        throw new GateRuntimeException("Error executing query for "+key,ex);
      }
      try {
        // get the result metadata so we know the column names
        rsmd = rs.getMetaData();
      } catch (SQLException ex) {
        throw new GateRuntimeException("Could not get result set metadata",ex);
      }
      columnNames = new ArrayList<String>();
      try {
        columnCount = rsmd.getColumnCount();
        //System.out.println("Column count is "+columnCount);
      } catch (SQLException ex) {
        throw new GateRuntimeException("Could not get column count",ex);
      }
      for(int i=1; i<=columnCount; i++) {
        String columnName = "";
        try {
          columnName = rsmd.getColumnName(i);
        } catch(SQLException ex) {
          throw new GateRuntimeException("Could not get column name",ex);
        }
        columnNames.add(columnName);
      }
      //System.out.println("Got column names: "+columnNames);
      int nrRows = 0;
      while(getNextRow(rs)) {
        nrRows++;
        columnValues = new ArrayList<Object>();
        for(int i=1; i<=columnCount; i++) {
          // get the value of the column 
          Object value = null;
          try {
            value = rs.getObject(i);
          } catch (SQLException ex) {
            throw new GateRuntimeException("Could not get value for column "+i,ex);
          }
          columnValues.add(value);
        }
        //System.out.println("Got column values: "+columnValues);
          
        if(getProcessingMode().equals(ProcessingMode.AddFeaturesFromFirst) ||
           getProcessingMode().equals(ProcessingMode.UpdateFeatures)) {
          // just set the features of the current annotation from the 
          // values from each column in the result set
          
          setFeaturesFromColumns(fm, columnNames, columnValues);
          
          // if we just wanted the values from the first row, exit this loop
          if(getProcessingMode().equals(ProcessingMode.AddFeaturesFromFirst)) {
            break;
          }
        } else if (getProcessingMode().equals(ProcessingMode.AddAnnotations)) {
          // create a new annotation for the value list
          FeatureMap newfm = Factory.newFeatureMap();
          setFeaturesFromColumns(newfm, columnNames, columnValues);
          gate.Utils.addAnn(outputAS, ann, getOutputAnnotationType(), newfm);
          
        }
      } // while getNextRow(rs)
      //System.out.println("Number rows we got: "+nrRows);
    } // if key is not empty
  }  

  
  protected void setFeaturesFromColumns(FeatureMap fm, List<String> colNames, List<Object> colValues) {
    for(int i=0; i<colNames.size(); i++) {
      String fname = colNames.get(i);
      String mappedName = (String)nameMappings.get(fname);
      if(mappedName != null) {
        fname = mappedName;
      }
      fm.put(fname,colValues.get(i));
    }
  }
  
  boolean getNextRow(ResultSet rs) {
    boolean haveNext = false;
    try {
      haveNext = rs.next();
    } catch(SQLException ex) {
      throw new GateRuntimeException("Error getting a row: ",ex);
    }
    return haveNext;
  }
  
  
  public enum ProcessingMode {
    AddFeaturesFromFirst,
    UpdateFeatures,
    AddAnnotations
  }

  @Override
  public void controllerExecutionStarted(Controller cntrlr) throws ExecutionException {
    if(nameMappings == null) {
      nameMappings = Factory.newFeatureMap();
    }
    prepareStatement(cntrlr);
  }

  
  
} // class JdbcLookup
