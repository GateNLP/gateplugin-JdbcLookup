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
 *  MapdbLookup.java
 *
 */
package com.jpetrak.gate.jdbclookup;


import gate.*;
import gate.api.AbstractDocumentProcessor;
import gate.creole.metadata.*;
import gate.util.Benchmark;
import gate.util.Benchmarkable;
import gate.util.GateRuntimeException;
import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


@CreoleResource(name = "MapdbLookup",
        comment = "Lookup features in a mapdb map")
public class MapdbLookup  extends AbstractDocumentProcessor implements Benchmarkable {

  private static final long serialVersionUID = 1L;
  
  
  
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


  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The to use for storing the looked-up value",
          defaultValue = "value")
  public void setValueFeature(String val) {
    this.valueFeature = val;
  }

  public String getValueFeature() {
    return valueFeature;
  }
  protected String valueFeature = "value";
  

  private LoadingMode loadingMode = LoadingMode.MEMORY_MAPPED;
  @Optional
  @RunTime
  @CreoleParameter(
      comment = "How to open/load the MapDB file",
      defaultValue = "MEMORY_MAPPED"
  )
  public void setLoadingMode(LoadingMode val) {
    loadingMode = val;
  }
  public LoadingMode getLoadingMode() {
    return loadingMode;
  }


  
  private URL mapDbFileUrl;
  @RunTime
  @CreoleParameter( 
          comment = "The URL of the MapDB file to use"
  )
  public void setMapDbFileUrl(URL u) {
    mapDbFileUrl = u;
  }
  public URL getMapDbFileUrl() { return mapDbFileUrl; }
  
  private String mapName = "map";
  @RunTime
  @Optional
  @CreoleParameter(
          comment = "The name of the map stored in the MapDB file",
          defaultValue = "map"
  )
  public void setMapName(String v) {
    mapName = v;
  }
  public String getMapName() { return mapName; }

  
  private FeatureMap featureMappings = null;
  /**
   * How to map sequence elements or map entries to the final features.
   * For mode SEQUENCE_TO_FEATURES this should contain the index of the 
   * sequence element as a key (0-based) and the name of the feature to
   * create for the element value as the value. For mode MAP_TOP_FEATURES,
   * this should contain the key of the input map as key and the name of
   * the target feature as the value. This is ignored if the mode is DIRECT.
   * 
   * @param fm TODO
   */
  @RunTime
  @Optional
  @CreoleParameter(
          comment = "Mapping information, if necessary."
  )
  public void setFeatureMappings(FeatureMap fm) {
    featureMappings = fm;
  }
  public FeatureMap getFeatureMappings() {
    return featureMappings;
  }
  
  private MappingMode mappingMode = MappingMode.DIRECT;
  /**
   * How to map the values from the mapdb map to target features.
   * Mode DIRECT will assign the whole object retrieved from the map
   * to a feature with the name specified for parameter valueFeature. 
   * In mode SEQUENCE_TO_FEATURES, the elements of the sequence with 
   * indices present in the featureMappings are assigned to features with
   * the name given in the featureMapping. If no featureMappings are specified,
   * the valueFeature name will be used with the element index appended.
   * In mode MAP_TP_FEATURES, the elements specified in the featureMapping
   * are assigned to features with the name from the featureMapping, if no
   * featureMapping is specified, all entries are copied over unchanged.
   * <p>
   * Note that for mode SEQUENCE_TO_FEATURES only the following types are 
   * supported: List&lt;?&gt;, double[], int[], and String[]. 
   * 
   * @param val TODO
   */
  @RunTime
  @Optional
  @CreoleParameter(
          comment = "How to map data from the mapdb to features",
          defaultValue = "DIRECT"
  )
  public void setMappingMode(MappingMode val) {
    mappingMode = val;
  }
  public MappingMode getMappingMode() {
    return mappingMode;
  }
  
  

  ////////////////////// FIELDS
  
  private DB db = null;
  private HTreeMap<String, Object> map = null;
  private static final Object syncObject = new Object();
  
  ////////////////////// PROCESSING
  
  @Override
  protected Document process(Document document) {
    
    AnnotationSet inputAS = null;
    if (inputASName == null
            || inputASName.isEmpty()) {
      inputAS = document.getAnnotations();
    } else {
      inputAS = document.getAnnotations(inputASName);
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

    fireStatusChanged("MapdbLookup: performing look-up in " + document.getName() + "...");

    if (containingAnns == null) {
      // go through all input annotations 
      for (Annotation ann : inputAnns) {
        doLookup(document, ann);
        if(isInterrupted()) {
          throw new GateRuntimeException("MapdbLookup has been interrupted");
        }
      }
    } else {
      // go through the input annotations contained in the containing annotations
      for (Annotation containingAnn : containingAnns) {
        AnnotationSet containedAnns = gate.Utils.getContainedAnnotations(inputAnns, containingAnn);
        for (Annotation ann : containedAnns) {
          doLookup(document, ann);
          if(isInterrupted()) { 
            throw new GateRuntimeException("MapdbLookup has been interrupted");
          }
        }
      }
    }

    fireProcessFinished();
    fireStatusChanged("MapdbLookup: look-up complete!");
    return document;
  }
  
  private void doLookup(Document doc, Annotation ann) {
    String key;
    long startTime = Benchmark.startPoint();
    FeatureMap fm = ann.getFeatures();
    if (getKeyFeature() == null || getKeyFeature().isEmpty()) {
      key = Utils.cleanStringFor(document, ann);
    } else {
      key = (String) fm.get(getKeyFeature());
    }
    if (key != null) {
      Object val = map.get(key);
      if(mappingMode == MappingMode.DIRECT) {
        fm.put(getValueFeature(), val);
      } else if(mappingMode == MappingMode.MAP_TO_FEATURES) {
        // in this case, val has to be a map!
        if(val instanceof Map) {
          Map<String,Object> valmap = (Map<String,Object>)val;
          // now if there are any mappings set, use them, otherwise just 
          // add all entries as is to the feature map
          if(getFeatureMappings()==null || getFeatureMappings().isEmpty()) {
            fm.putAll(valmap);
          } else {
            throw new GateRuntimeException("feature mappings not implemented yet for MAP_TO_FEATURES");
          }
        } else {
          throw new RuntimeException("Cannot use mapping mode MAP_TO_FEATURES, value is not a Map but "+val.getClass());
        }
      } else if(mappingMode == MappingMode.SEQUENCE_TO_FEATURES) {
        // for this the value must be an array or a list.
        // If there are no featureMappings we will generate feature names
        // <getValueFeature()>N, otherwise we will use the mappings
        if(val instanceof List) {
          List<Object> toAdd = (List<Object>)val;
          for(int i=0; i<toAdd.size(); i++) {
            addMappedElement(fm,toAdd.get(i),i,getFeatureMappings());
          }
        } else if(val instanceof double[]) {
          double[] toAdd = (double[])val;
          for(int i=0; i<toAdd.length; i++) {
            addMappedElement(fm,toAdd[i],i,getFeatureMappings());
          }          
        } else if(val instanceof int[]) {
          int[] toAdd = (int[])val;
          for(int i=0; i<toAdd.length; i++) {
            addMappedElement(fm,toAdd[i],i,getFeatureMappings());
          }          
        } else if(val instanceof String[]) {
          String[] toAdd = (String[])val;
          for(int i=0; i<toAdd.length; i++) {
            addMappedElement(fm,toAdd[i],i,getFeatureMappings());
          }                    
        } else {
          throw new RuntimeException("Cannot use mapping mode SEQUENCE_TO_FEATURES, value is not supported: "+val.getClass());
        }
      }
    }
    benchmarkCheckpoint(startTime, "__MapdbLookup");
  }
  
  // Helper function to add sequence elements to a target feature map, optionally
  // using the mappings
  private void addMappedElement(FeatureMap targetFm, Object value, int index, FeatureMap mappings) {
    if(mappings==null || mappings.isEmpty()) {
      // generate the names
      String name = getValueFeature();
      if(name == null) name = getMapName();
      name = name + index;
      targetFm.put(name,value);
    } else {
      String idx = ""+index;
      String name = (String)mappings.get(idx);
      if(name != null) {
        targetFm.put(name,value);
      }
    }
  }

  @Override
  protected void beforeFirstDocument(Controller ctrl) {
    synchronized (syncObject) {
      db = (DB) sharedData.get("db");
      if (db != null) {
        System.err.println("INFO: shared db already opened in duplicate " + duplicateId + " of PR " + this.getName());
        map = (HTreeMap<String, Object>) sharedData.get("map");
      } else {
        long startTime = Benchmark.startPoint();
        System.err.println("INFO: Opening DB in duplicate " + duplicateId + " of PR " + this.getName());
        File file = gate.util.Files.fileFromURL(mapDbFileUrl);
        if (getLoadingMode() == null || getLoadingMode() == LoadingMode.MEMORY_MAPPED) {
          db = DBMaker.fileDB(file).fileMmapEnable().readOnly().make();
          map = (HTreeMap<String, Object>) db.hashMap(getMapName()).open();
        } else if(getLoadingMode() == LoadingMode.FILE_ONLY) {
          db = DBMaker.fileDB(file).readOnly().make();
          map = (HTreeMap<String, Object>) db.hashMap(getMapName()).open();
        } else if(getLoadingMode() == LoadingMode.COPY2MEMORY) {
          DB tmpdb = DBMaker.fileDB(file).readOnly().make();
          HTreeMap<String, Object> fmap = (HTreeMap<String, Object>)tmpdb.hashMap(getMapName()).create();
          db = DBMaker.memoryDB().make();
          map = (HTreeMap<String, Object>)db.hashMap(getMapName()).create();
          map.putAll(fmap);
          tmpdb.close();
        }
        sharedData.put("db", db);
        sharedData.put("map", map);
        benchmarkCheckpoint(startTime, "__LoadMapdb");
        //System.err.println("GOT map: "+map.size());
      }
    }
  }

  @Override
  protected void afterLastDocument(Controller ctrl, Throwable t) {
  }

  @Override
  protected void finishedNoDocument(Controller ctrl, Throwable t) {
  }
  
  @Override
  public void cleanup() {
    if(duplicateId == 0 && db!=null && !db.isClosed()) { db.close(); }
  }
  
  protected void benchmarkCheckpoint(long startTime, String name) {
    if (Benchmark.isBenchmarkingEnabled()) {
      Benchmark.checkPointWithDuration(
              Benchmark.startPoint() - startTime,
              Benchmark.createBenchmarkId(name, this.getBenchmarkId()),
              this, null);
    }
  }
  
  @Override
  public String getBenchmarkId() {
    return benchmarkId;
  }

  @Override
  public void setBenchmarkId(String string) {
    benchmarkId = string;
  }
  private String benchmarkId = this.getName();
  

  
  public static enum LoadingMode {
    MEMORY_MAPPED,
    FILE_ONLY,
    COPY2MEMORY
  }

  public static enum MappingMode {
    DIRECT,
    SEQUENCE_TO_FEATURES,
    MAP_TO_FEATURES
  }
  
  
} // class JdbcLookup
