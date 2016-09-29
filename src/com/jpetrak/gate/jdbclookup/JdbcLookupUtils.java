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
