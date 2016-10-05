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
import gate.creole.ExecutionInterruptedException;
import gate.creole.metadata.*;
import gate.util.GateRuntimeException;
import java.io.File;
import java.net.URL;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


@CreoleResource(name = "MapdbLookup",
        comment = "Lookup features in a mapdb map")
public class MapdbLookup  extends AbstractDocumentProcessor {
  
  
  
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


  ////////////////////// FIELDS
  
  DB db = null;
  HTreeMap<String, Object> map = null;
  
  
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
    FeatureMap fm = ann.getFeatures();
    if (getKeyFeature() == null || getKeyFeature().isEmpty()) {
      key = Utils.cleanStringFor(document, ann);
    } else {
      key = (String) fm.get(getKeyFeature());
    }
    if (key != null) {
      Object val = map.get(key);
      fm.put(getValueFeature(), val);
    }
  }
  

  @Override
  protected void beforeFirstDocument(Controller ctrl) {    
    File file = gate.util.Files.fileFromURL(mapDbFileUrl);
    db = DBMaker.fileDB(file).fileMmapEnable().readOnly().make();
    //System.err.println("DB openend: "+db);
    map = (HTreeMap<String,Object>)db.hashMap(getMapName()).
            open();
    //System.err.println("GOT map: "+map.size());
  }

  @Override
  protected void afterLastDocument(Controller ctrl, Throwable t) {
    if(db!=null) db.close();
  }

  @Override
  protected void finishedNoDocument(Controller ctrl, Throwable t) {
    if(db!=null) db.close();
  }
  

  
  
} // class JdbcLookup
