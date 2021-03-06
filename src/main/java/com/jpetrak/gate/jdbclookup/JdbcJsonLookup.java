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

import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;
import gate.FeatureMap;
import gate.Resource;
import gate.creole.ResourceInstantiationException;
import gate.creole.metadata.CreoleParameter;
import gate.creole.metadata.CreoleResource;
import gate.creole.metadata.RunTime;
import gate.util.GateRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gate.Factory;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: catch any exception we throw in execute() and attempt to close the
// connection properly so we do not leak connections in case of an error



/**
 * PR to lookup Json data from a JDBC database and create annotations from it.
 * The PR will use the value of a feature or the string from an annotation 
 * to lookup up matching rows in the database. The result rows are expected to
 * contain a single String value which is expected to be a JSON data structure.
 * Depending on the processing mode, the JSON data structure which is expected
 * to be a map or an array of maps is used to add features to the original
 * annotation or create additional annotation(s). 
 * If the processing mode is to set or update the existing features and the
 * JSON is a list of maps, then only the first element in that list is used.
 * If the processing mode is adding annotations, then a new annotation is created
 * for each element of the list, and the features are set according to the 
 * map the element represents.
 * 
 * NOTE: for now this expects to get only one database row at most for each key! If 
 * there is more than one row, only the first one is used!!
 * 
 * @author Johann Petrak
 */
@CreoleResource(name = "JdbcJsonLookup",
        comment = "Lookup Json in a JDBC table and create features and/or annotations from the results")
public class JdbcJsonLookup extends JdbcLookupBase
{

  // ***
  // *** PR Parameters
  // ***
  // Mode
  protected ProcessingMode processingMode = ProcessingMode.AddAnnotations;

  @RunTime
  @CreoleParameter(
          comment = "Processing mode",
          defaultValue = "AddAnnotations")
  public void setProcessingMode(ProcessingMode mode) {
    processingMode = mode;
  }

  public ProcessingMode getProcessingMode() {
    return processingMode;
  }
  
  
  
  private ObjectMapper mapper = new ObjectMapper();

  private static AtomicInteger dupNumber;

  public JdbcJsonLookup() {
    logger = Logger.getLogger(this.getClass().getName());
  }

  @Override
  public synchronized Resource init() throws ResourceInstantiationException {
    if(dupNumber == null) {
      dupNumber = new AtomicInteger(0);
    }
    int ourNum = dupNumber.getAndIncrement();
    this.getFeatures().put("DuplicationNumber", ourNum); // we may use this as $pr{MyPrNr} for duplication
    super.init();
    return this;
  }

  @Override
  public void doLookup(Document doc, Annotation ann, AnnotationSet outputAS) {
    // first get the key: either the content of some feature of the 
    // underlying document text, if the feature name is empty
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
      // look up the key in the database
      String json = (String) getJsonForKey(key);
      int listId;
      if (json != null && !json.isEmpty()) {
        Object arrayOrMap = null;
        try {
          // parse the JSON into a Java object
          arrayOrMap = mapper.readValue(json, Object.class);
        } catch (Exception ex) {
          throw new GateRuntimeException("Could not parse JSON: " + json, ex);
        }
        if (processingMode.equals(ProcessingMode.AddAnnotations)) {
          ArrayList<Integer> theIds = new ArrayList<Integer>();
          if (arrayOrMap instanceof Map) {
            theIds.add(addLookup(ann, (Map) arrayOrMap, outputAS, outputType, fm));
          } else {
            ArrayList<Object> theList = (ArrayList<Object>) arrayOrMap;
            for (Object member : theList) {
              // all the members must be Maps!
              if (member instanceof Map) {
                theIds.add(addLookup(ann, (Map) member, outputAS, outputType, fm));
              } else {
                throw new GateRuntimeException("Odd JSON array does not contain just maps: " + json);
              }
            }
          }
          if (!listType.isEmpty()) {
            FeatureMap fmList = Factory.newFeatureMap();
            fmList.putAll(fm); // inherit the original Lookup annotation features
            fmList.put("ids",theIds);
            //fmList.put("debugJdbcLookup",fm);
            listId = gate.Utils.addAnn(outputAS, ann, listType, fmList);
            // add the id of the list annotation to the feature map of each candidate
            // as feature "llId" 
            for(int id : theIds) {
              Annotation cand = outputAS.get(id);
              cand.getFeatures().put("llId",listId);
            }
          }
        } else {
          if (arrayOrMap instanceof List) {
            ArrayList arr = (ArrayList) arrayOrMap;
            if (!arr.isEmpty()) {
              arrayOrMap = arr.get(0);
            } else {
              arrayOrMap = null;
            }
          }
          if (arrayOrMap != null) {
            Map map = (Map) arrayOrMap;
            if (processingMode.equals(ProcessingMode.AddFeatures)) {
              FeatureMap newfm = gate.Factory.newFeatureMap();
              newfm.putAll(map);
              newfm.putAll(fm);
              ann.setFeatures(newfm);
            } else {
              fm.putAll(map);
              ann.setFeatures(fm);
            }
          }
        }
      }
    }
  }

  protected String getJsonForKey(String key) {
    try {
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
    boolean haveResult = false;
    try {
      haveResult = rs.next();
    } catch(SQLException ex) {
      throw new GateRuntimeException("Error checking for result rows",ex);
    }
    String ret = "";
    if(haveResult) {
      try {
        ret = rs.getString(1);
      } catch (SQLException ex) {
        throw new GateRuntimeException("Could not get field number 1 from result row",ex);
      }
    }
    return ret;
  }
  
  protected int addLookup(Annotation ann, Map theMap, AnnotationSet outputAS, String outputType, FeatureMap parentFeatures) {
    // create a new annotation in the output annotation set
    // and set the features from the map
    // if parentFeatures is non-null, then the features from that map will be added too,
    // but overriden by the features in theMap
    FeatureMap fm = Factory.newFeatureMap();
    if(parentFeatures != null) {
      fm.putAll(parentFeatures);      
    }
    fm.putAll(theMap);
    return gate.Utils.addAnn(outputAS, ann, outputType, fm);
  }


  public enum ProcessingMode {
    AddFeatures,
    UpdateFeatures,
    AddAnnotations
  }
}
