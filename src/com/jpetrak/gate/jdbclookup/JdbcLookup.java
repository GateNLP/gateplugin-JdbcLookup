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
  @CreoleParameter(comment="Map from SQL column names to feature names. Target types can be added after the name, separated by a pipe symbol: |")
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
  private List<Object> columnValues = null;
  private int columnCount = 0;
  
  
  // This is used for caching the result columns from the db query
  List<String> resultColumns = null;
  
  // This is used for caching the feature names for the columns
  List<String> resultFeatures = null;
  
  // This is used for caching the types for the columns
  List<String> resultTypes = null;
   
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
      
      
      // TODO: cache the metadata since it should be the same for all results
      // instead of keeping rsmd around, store the column names in a list
      
      // if we still do not have our caches create them
      if (resultColumns == null) {
        try {
          // get the result metadata so we know the column names
          rsmd = rs.getMetaData();
        } catch (SQLException ex) {
          throw new GateRuntimeException("Could not get result set metadata", ex);
        }
        resultColumns = new ArrayList<String>();
        try {
          columnCount = rsmd.getColumnCount();
          //System.out.println("Column count is "+columnCount);
        } catch (SQLException ex) {
          throw new GateRuntimeException("Could not get column count", ex);
        }
        for (int i = 1; i <= columnCount; i++) {
          String columnName = "";
          try {
            columnName = rsmd.getColumnName(i);
          } catch (SQLException ex) {
            throw new GateRuntimeException("Could not get column name", ex);
          }
          resultColumns.add(columnName);
        }
        //System.out.println("Got column names: "+columnNames);
        // Now is also a good time to cache the column name mappings
        resultFeatures = new ArrayList<String>(resultColumns.size());
        resultTypes = new ArrayList<String>(resultColumns.size());
        for(int k=0; k<resultColumns.size(); k++) {
          String column = resultColumns.get(k);
          String mapping = (String)nameMappings.get(column);
          if(mapping == null) {
            resultFeatures.add(column);
            resultTypes.add("");
          } else {
            // first check if the mapping has a type added
            // for now the only thing supported is |s2adouble!!
            String name = mapping;
            String type = "";
            if (mapping.endsWith("|s2adouble")) {
              type = "s2adouble";
              name = mapping.substring(0,mapping.length()-"|s2adouble".length());
            } else if(mapping.endsWith("|s2ldouble")) {
              type = "s2ldouble";
              name = mapping.substring(0,mapping.length()-"|s2ldouble".length());
            }
            resultFeatures.add(name);
            resultTypes.add(type);
          }
        }
      }
      
      ///
      
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
          
          setFeaturesFromColumns(fm, columnValues);
          
          // if we just wanted the values from the first row, exit this loop
          if(getProcessingMode().equals(ProcessingMode.AddFeaturesFromFirst)) {
            break;
          }
        } else if (getProcessingMode().equals(ProcessingMode.AddAnnotations)) {
          // create a new annotation for the value list
          FeatureMap newfm = Factory.newFeatureMap();
          setFeaturesFromColumns(newfm, columnValues);
          gate.Utils.addAnn(outputAS, ann, getOutputAnnotationType(), newfm);
          
        }
      } // while getNextRow(rs)
      //System.out.println("Number rows we got: "+nrRows);
    } // if key is not empty
  }  

  
  protected void setFeaturesFromColumns(FeatureMap fm, List<Object> colValues) {
    for(int i=0; i<colValues.size(); i++) {
      String type = resultTypes.get(i);
      String fname = resultFeatures.get(i);
      Object value = colValues.get(i);
      if("s2adouble".equals(type)) {
        double[] doubles;
        if(value==null) {
          doubles = new double[0];
        } else {
          String els = value.toString();
          els = els.substring(1,els.length()-1);
          //System.err.println(els);
          String[] elss = els.split(",\\s+");
          doubles = new double[elss.length];
          for(int k=0; k<doubles.length; k++) {
            doubles[k] = Double.parseDouble(elss[k]);
          }
        }
        //System.err.println("fn="+fname+", d="+doubles);
        fm.put(fname,doubles);
      } else if("s2ldouble".equals(type)) {
        List<Double> doubles;
        if(value==null) {
          doubles = new ArrayList<Double>(0);
        } else {
          String els = value.toString();
          els = els.substring(1,els.length()-1);
          //System.err.println(els);
          String[] elss = els.split(",\\s+");
          doubles = new ArrayList<Double>(elss.length);
          for(int k=0; k<doubles.size(); k++) {
            doubles.add(Double.parseDouble(elss[k]));
          }
        }
        //System.err.println("fn="+fname+", d="+doubles);
        fm.put(fname,doubles);
      } else {
        fm.put(fname,colValues.get(i));
      }
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
