@Grab("org.mapdb#mapdb;3.0.2")
import org.mapdb.*

// Simple Groovy script to convert an embeddings file in text format
// to  mapdb file that can be used with the MapdbLookup PR to add
// embedding vectors to annotations.
//
// The mapdb file will contain two hash maps:
// "config" contains the following entries:
//   * n: number of columns/dimension of the embedding vectors
// "word2vec": maps each word (String) to an embedding vector (array of double)
//
// This script will either create the mapdb file or add to an existing file,
// if the file already exists and contains embedding then some checks will be made 
// if the dimensions are compatible.
//
// The script reads from standard input and writes to the file specified as an arguments.
// The format expected is textual word2vec format:
// line 1 contains the the number of embedding rows white space and the number of dimensions
// the remaining lines contain the word, white space and as many float numbers as dimensions

import groovy.json.*

if(args.size() != 3) {
  System.err.println("Need exactly three arguments: mapdb-file startSize incrementSize")
  System.err.println("  mapdb-file will be created or updated")
  System.err.println("  statSize and incrementSize can be in the form of 1073741824 or 1048576k or 1024m or 1g")
  System.exit(1)
}

startSize = args[1]
incrementSize = args[2]

def size2Long(s) {
  s = s.toLowerCase()
  f = 1
  if(s.endsWith("m")) {
    f = 1024 * 1024
    s = s.substring(0,s.size()-1)
  } else if(s.endsWith("g")) {
    f = 1024 * 1024 * 1024
    s = s.substring(0,s.size()-1)
  } else if(s.endsWith("k")) {
    f = 1024
    s = s.substring(0,s.size()-1)
  }
  r = s as Long
  r = r * f
  return r
}


startSize = size2Long(startSize)
incrementSize = size2Long(incrementSize)

System.err.println("Initial size in bytes: "+startSize)
System.err.println("Increment size in bytes: "+incrementSize)

// db = DBMaker.fileDB(args[0]).fileMmapEnableIfSupported().make()
db =  DBMaker.
  fileDB(args[0]).
  fileMmapEnable().
  closeOnJvmShutdown().
  allocateStartSize(startSize).
  allocateIncrement(incrementSize).
  make()
config = db.hashMap("config").createOrOpen();
map = db.hashMap("word2vec").keySerializer(Serializer.STRING).valueSerializer(Serializer.DOUBLE_ARRAY).createOrOpen();

oldDims = null

if(map.size()>0) {
  oldDims = config.get("n")
  System.err.println("WARNING: already contains "+map.size()+" embeddings with dimensions: "+oldDims);
  if(oldDims == null) {
    db.close()
    System.err.println("ERROR: we have embeddings but no config info stored")
    System.exit(1);
  }
} 

linenr = 0
hrows = -1
hcols = -1

headerpattern = ~/^\s*(\d+)\s+(\d+)\s*$/

System.in.eachLine() { line ->
  linenr++
  if(linenr > 2 && (linenr % 1000) == 0) {
    db.commit()
    System.err.println("Lines read: "+linenr)
  }
  if(linenr==1) {
    kv = ""
    // process header or get dimensions from first row if there is no header
    m=headerpattern.matcher(line)
    if(m.matches()) {
      hrows=m.group(1) as int
      hcols=m.group(2) as int
    } else {
      kv = line.split('\\s+')
      hcols = kv.size()-1
      hrows = "unknown (no header line)"
    }
    // check/store dimensions  
    System.err.println("INFO: processing file with rows: "+hrows+", dimensions: "+hcols)
    if(oldDims != null) {
      if(oldDims != hcols) {
        db.close()
        System.err.println("ERROR: already have embeddings of size "+oldDims+" but ones to add have size "+hcols)
        System.exit(1)
      }
    }
    config.put("n",hcols)
    if(kv) {
      // if we had no header need to process the line
      addLine(line,linenr,hcols)
    }
  } else {
    addLine(line,linenr,hcols)
  }
}
System.err.println("INFO: finished, read lines (including header line, if any): "+linenr)
db.close()

def addLine(String line, int linenr, int hcols) {
    // process embedding line
    // first split into word and rest
    kv = line.split('\\s+',2)
    if(kv.size() != 2) {
      System.err.println("Odd line "+linenr+", line length is: "+line.size())
      System.err.println("Odd line ignored: >"+line+"<")
      return
    }
    word = kv[0]
    vec = kv[1]
    numbers = vec.split('\\s+')
    if(numbers.size() != hcols) {
      System.err.println("Odd line "+linenr+", has cols: "+numbers.size()+", ignored: >"+line+"<")
      return
    }
    double[] embvec = new double[numbers.size()]
    //System.err.println("DEBUG numbers="+numbers)
    numbers.eachWithIndex { s,i -> embvec[i] = s.toDouble() }
    map.put(word,embvec)
}


