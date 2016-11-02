@Grab("org.mapdb#mapdb;3.0.2")
import org.mapdb.*

// Simple Groovy script to recover a mapdb file that has not been closed
// properly, if possible.

import groovy.json.*

if(args.size() != 1) {
  System.err.println("Need one argument: mapdb-file")
  System.err.println("  mapdb-file must exist ")
  System.exit(1)
}

db =  DBMaker.fileDB(args[0]).checksumHeaderBypass().make()
db.close()

