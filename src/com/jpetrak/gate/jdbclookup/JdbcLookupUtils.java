
package com.jpetrak.gate.jdbclookup;

import gate.Resource;
import gate.util.GateRuntimeException;
import java.util.Collection;

/**
 * Some useful utility methods for use in JAPE etc.
 * 
 * 
 * 
 * @author Johann Petrak
 */
public class JdbcLookupUtils {
  public static String get(Resource lr, String key) {
    if(lr instanceof JdbcString2StringLR) {
      return ((JdbcString2StringLR)lr).get(key);
    } else {
      throw new GateRuntimeException("Resource is not a JdbcString2StringLR");
    }
  }
  public static boolean contains(Resource lr, String key) {
    if(lr instanceof JdbcString2StringLR) {
      return ((JdbcString2StringLR)lr).contains(key);
    } else {
      throw new GateRuntimeException("Resource is not a JdbcString2StringLR");
    }
  }
  public static String put(Resource lr, String key, String value) {
    if(lr instanceof JdbcString2StringLR) {
      return ((JdbcString2StringLR)lr).put(key, value);
    } else {
      throw new GateRuntimeException("Resource is not a JdbcString2StringLR");
    }
  }
  public static void removeAll(Resource lr, Collection<String> keys) {
    if(lr instanceof JdbcString2StringLR) {
      ((JdbcString2StringLR)lr).removeAll(keys);
    } else {
      throw new GateRuntimeException("Resource is not a JdbcString2StringLR");
    }    
  }
  
}
