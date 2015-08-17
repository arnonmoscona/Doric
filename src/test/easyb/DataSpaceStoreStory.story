/*
 * Copyright (c) 2015. Arnon Moscona
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import com.moscona.util.monitoring.stats.SimpleStatsService

import static com.moscona.test.easyb.TestHelper.*
import com.moscona.dataSpace.persistence.DirectoryDataStore
import com.moscona.dataSpace.exceptions.DataSpaceException
import com.moscona.dataSpace.stub.MemoryManager
import com.moscona.dataSpace.DataSpace
import com.moscona.dataSpace.impl.*
import com.moscona.dataSpace.Text
import com.moscona.dataSpace.Numeric
import com.moscona.dataSpace.DataFrame
import com.moscona.test.easyb.DelegatingIterator

before_each "scenario", {
  given "a temporary location", {
    location = tempFile("dataStore").absolutePath
    new File(location).delete()
  }
  and "a stats service", {
    stats = new SimpleStatsService();
  }
  and "a data store based on it", {
    printExceptions {
      store = new DirectoryDataStore(location,true,stats)
    }
  }
  and "a mock memory manager", {
    mockMemoryManager = new MemoryManager()
  }

  // important: the first data store should not be "contaminated" by the vectors we put in the second data store

  and "a second temporary location", {
    location2 = tempFile("dataStore").absolutePath
    new File(location2).delete()
  }
  and "a second stats service", {
    stats2 = new SimpleStatsService();
  }
  and "a second data store based on it", {
    printExceptions {
      store2 = new DirectoryDataStore(location2,true,stats2)
    }
  }
  and "a data space", {
    dataSpace2 = new DataSpace(store2, mockMemoryManager)
  }
  and "a segment size of 10", {
    dataSpace2.segmentSize = 10
  }
  and "a double vector with 32 values", {
    doubleVector = new DoubleVector(dataSpace2);
    (1..32).each{doubleVector.append(it as double)}
    doubleVector.seal()
  }
  and "a long vector with 32 values", {
    longVector = new LongVector(dataSpace2);
    (1..32).each{longVector.append(it as long)}
    longVector.seal()
  }
  and "an integer vector with 32 values", {
    integerVector = new IntegerVector(dataSpace2);
    (1..32).each{integerVector.append(it as int)}
    integerVector.seal()
  }
  and "a logical vector with 32 values", {
    logicalVector = new LogicalVector(dataSpace2);
    def b = true
    (1..32).each {
      logicalVector.append(b as boolean)
      b = !b
    }
    logicalVector.seal()
  }
  and "a string vector with 32 values", {
    stringVector = new StringVector(dataSpace2);
    (1..9).each{stringVector.append("0"+it.toString())}
    (10..32).each{stringVector.append(it.toString())}
    stringVector.seal()
  }
  and "a result place holder", { result = null }
}

after_each "scenario", {
  store.close()
  deleteAllRecursively location
  new File(location as String).deleteDir()
  store2.close()
  deleteAllRecursively location2
  new File(location2 as String).deleteDir()
}

after "all", {
  clearTmpDir()
}

scenario "immediately after opening for write", {
  then "the store directory should exist", {
    def dir = new File(location as String)
    dir.exists().shouldBe true
    dir.isDirectory().shouldBe true
  }
  and "the metadata file should exist", {
    new File(location+"/store.metadata.yml").exists().shouldBe true
  }
  and "the store should be empty", {
    store.isEmpty().shouldBe true
  }
  and "the segment size should be null", {
    store.segmentSize.shouldBe null
  }
  and "the store should be writable", {
    store.canWrite().shouldBe true
  }
  and "I should not be able to open another store against the same location (write mode)", {
    ensureThrows(DataSpaceException) {
      store = new DirectoryDataStore(location,true,stats)
    }
  }
  and "I should not be able to open another store against the same location (read mode)", {
    ensureThrows(DataSpaceException) {
      store = new DirectoryDataStore(location,false,stats)
    }
  }
  and "if I close it I should be able to open another store using the same location", {
    ensureDoesNotThrow(Exception) {
      store.close();
      store = new DirectoryDataStore(location,false,stats)
    }
  }
  and "the next segment ID should be 1", {
    store.nextSegmentId.shouldBe 1
  }
  and "the next segment ID (second call) should be 2", {
    store.nextSegmentId.shouldBe 2
  }
}

scenario "read locks", {
  when "I close the store", {
    store.close()
  }
  then "I should be able to open two separate read only stores against the location", {
    ensureDoesNotThrow(Exception) {
      store1 = new DirectoryDataStore(location,false,stats,"first")
      store2 = new DirectoryDataStore(location,false,stats,"second")
      store1.close()
      store2.close()
    }
  }
  and "I should not be able to open a read only one and then a write one against the same location", {
    ensureThrows(DataSpaceException) {
      store1 = new DirectoryDataStore(location,false,stats,"first-shared")
      try {
        store2 = new DirectoryDataStore(location,true,stats,"second-exclussive")
      }
      catch (Exception e) {
        store1.close() // make sure that the shared lock is removed regardless
        throw e
      }
      store1.close()
      store2.close()
    }
  }
}

scenario "path calculations", {
  then "small segment numbers are stored under the segments directory", {
    store.getBackingArrayPathForSegment(1,"bytes").shouldBe location+"/segments/1.bytes"
    store.getBackingArrayPathForSegment(12,"ints").shouldBe location+"/segments/12.ints"
  }
  and "segment number 121 is stored in the 12 directory", {
    store.getBackingArrayPathForSegment(121,"strings").shouldBe location+"/segments/12/121.strings"
  }
  and "segment 1201 is also stored in the 12 directory", {
    store.getBackingArrayPathForSegment(1201,"doubles").shouldBe location+"/segments/12/1201.doubles"
  }
  and "segment 12345 is stored in the 12/34 directory", {
    store.getBackingArrayPathForSegment(12345,"bytes").shouldBe location+"/segments/12/34/12345.bytes"
  }
}


scenario "saving and restoring temporary backing arrays (white box test)", {
  given "a sealed temporary integer vector", {
    /* nothing to do */
  }
  then "all its segments should be on disk", {
    ensureDoesNotThrow(Exception) {
      (new DelegatingIterator(integerVector.segmentIterator())).each { info ->
        def path = store2.getBackingArrayPathForSegment(info.segment.dataStoreSegmentId, "INTEGER.tmp") as String
        new File(path).exists().shouldBe true
      }
    }
  }
  and "if I swap out all of its segments", {
    ensureDoesNotThrow(Exception) {
      (new DelegatingIterator(integerVector.segmentIterator())).each { info ->
        info.segment.swapOut()
      }
    }
  }
  then "the segments should really be swapped out", {
    ensureDoesNotThrow(Exception) {
      (new DelegatingIterator(integerVector.segmentIterator())).each { info ->
        info.isBackingArrayLoaded().shouldBe false
      }
    }
  }
  and "then I should still be able to iterate over the elements", {
    l = []
    (0..31).each {
      l << integerVector.get(it).toString()
    }
    l.join(",").shouldBe "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32"
  }
}

scenario "making temporary vectors into persistent vectors (white box test)", {
  given " a string to remember the original name", {
    originalName = "foo"
    dataSpace2.assign(originalName,stringVector)
  }
  when "I assign the string vector to the persistent name space", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.assign("strings", stringVector)
    }
  }
  then "it should no longer be in the temporary name space", {
    ensureThrows(DataSpaceException) {
      dataSpace2.tempNameSpace.get("foo")
    }
  }
  and "I should not be able to get it via the 'non qualified' name", {
    ensureThrows(DataSpaceException) {
      dataSpace2.get(originalName)
    }
  }
  and "I should be able to access it from the data space direct API by using a 'qualified' name", {
    dataSpace2.get("__persistent.strings").shouldNotBe null
  }
  and "all its segments should be renamed to exclude the .tmp extension", {
      (new DelegatingIterator(stringVector.segmentIterator())).each { info ->
        def path = null
        ensureDoesNotThrow(Exception) {
          path = info.segment.backingArrayStorageLocation
        }
        def namedCorrectly = ! path.endsWith(".tmp")
        def exists = new File(path as String).exists()
        def oldExists = new File("${path}.tmp").exists()
        "\nlocation correct: $namedCorrectly, exists: $exists, old location exists: $oldExists\n".shouldBe "\nlocation correct: true, exists: true, old location exists: false\n"
    }
  }
  and "I should still be able to iterate over it", {
    l = []
    (0..31).each {
      l << integerVector.get(it).toString()
    }
    l.join(",").shouldBe "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32"
  }
  and "all its segment files should not be marked tmp", {
    def list = new File(location2+"/segments").listRecursive().findAll{it.contains(".STRING")}
    list.size().shouldBe 4
    list.findAll{it.endsWith(".tmp")}.size().shouldBe 0
  }
}

scenario "restoring a data space using a read-write data store", {
  given "that the string vector is persistent as 'names'", {
    dataSpace2.persistentNameSpace.assign("names",stringVector)
  }
  and "the integer vector is persistent as 'scores'", {
    dataSpace2.persistentNameSpace.assign("scores",integerVector)
  }
  and "the double vector is temporary as 'temps'", {
    dataSpace2.assign("temps",doubleVector)
  }
  when "I restore the data space", {
    ensureDoesNotThrow(Exception) {
      result = store2.loadDataSpace(mockMemoryManager)
    }
  }
  then "the string vector should be accessible", {
    ensureDoesNotThrow(Exception) {
      result.persistentNameSpace.get("names")
    }
  }
  and "the integer vector should be accessible", {
    ensureDoesNotThrow(Exception) {
      result.get("__persistent.scores")
    }
  }
  and "the double vector should not be there any more", {
    ensureThrows(DataSpaceException) {
      result.get("temps")
    }
  }
  and "I should be able to iterate over the string vector", {
    sv = result.persistentNameSpace.get("names")
    l = []
    (0..31).each { i->
      printExceptions {
        l << sv.get(i).toString()
      }
    }
    l.join(",").shouldBe "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32"
  }
  and "the flush count should increment by two", {
    stats2.getStat("DirectoryDataStore.dump(DataSpace)").descriptiveStatistics.count.shouldBe 3
  }
}

scenario "restoring a data space using a read-only data store", {
  given "that the string vector is persistent as 'names'", {
    dataSpace2.persistentNameSpace.assign("names",stringVector)
  }
  and "the integer vector is persistent as 'scores'", {
    dataSpace2.persistentNameSpace.assign("scores",integerVector)
  }
  and "the double vector is temporary as 'temps'", {
    dataSpace2.assign("temps",doubleVector)
  }
  and "I close the read-write data store", {
    store2.close()
  }
  and "a read-only data store", {
    readOnlyStore = null
    ensureDoesNotThrow(Exception) {
      readOnlyStore = new DirectoryDataStore(location2,false,stats2)
    }
  }
  when "I restore the data space", {
    ensureDoesNotThrow(Exception) {
      result = readOnlyStore.loadDataSpace(mockMemoryManager)
    }
  }
  then "the string vector should be accessible", {
    ensureDoesNotThrow(Exception) {
      result.persistentNameSpace.get("names")
    }
  }
  and "the integer vector should be accessible", {
    ensureDoesNotThrow(Exception) {
      result.get("__persistent.scores")
    }
  }
  and "the double vector should not be there any more", {
    ensureThrows(DataSpaceException) {
      result.get("temps")
    }
  }
  and "I should be able to iterate over the string vector", {
    sv = result.persistentNameSpace.get("names")
    l = []
    (0..31).each {
      l << sv.get(it).toString()
    }
    l.join(",").shouldBe "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32"
  }
}

scenario "moving several data elements at once from the temporary name space to the persistent name space", {
  given "some scalars and vectors", {
    objects = [
      "text": new Text("text scalar"),
      "int1": new Numeric<Integer>(1),
      "int2": new Numeric<Integer>(2),
      "stringVector": stringVector,
      "doublevector": doubleVector
    ]
  }
  when "I move several data items to the persistent namespace", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.assignAll(objects)
    }
  }
  then "they should all be there", {
    def l = []
    l.addAll(dataSpace2.persistentNameSpace.keySet())
    l.sort().join(", ").shouldBe "doublevector, int1, int2, stringVector, text"
  }
  and "the flush count should increment by one", {
    stats2.getStat("DirectoryDataStore.dump(DataSpace)").descriptiveStatistics.count.shouldBe 1
  }
}

// data frames as collections of vectors, and what happens when persistence types are inconsistent

scenario "moving a data frame into the persistent name space", {
  given "a data frame with some of our vectors", {
    df = new DataFrame(dataSpace2)
    df.cbind("rowNum",integerVector)
    df.cbind("price",doubleVector)
  }
  when "I move it to the persistent name space", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.assign("table",df)
    }
  }
  then "all its vectors should also have moved into the persistent name space", {
    def l = []
    l.addAll(dataSpace2.persistentNameSpace.keySet())
    l.sort().join(", ").shouldBe "__DoubleVector_1, __IntegerVector_2, table"
  }
}

scenario "binding vectors from the temporary name space into a data frame that's in the persistent name space", {
  given " a data frame", {
    df = new DataFrame(dataSpace2)
  }
  when "I move it to the persistent name space", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.assign("table",df)
    }
  }
  and "I add vectors from the temporary name space to it", {
    df.cbind("price",doubleVector)
    df.cbind("rowNum",integerVector)
  }
  then "the vectors should have been moved to the persistent name space", {
    def l = []
    l.addAll(dataSpace2.persistentNameSpace.keySet())
    l.sort().join(", ").shouldBe "__DoubleVector_1, __IntegerVector_2, table"
  }
}

scenario "binding vectors from a higher persistence type to a data frame from a lower persistence type", {
  given "that some vectors are in the persistent name space", {
    dataSpace2.persistentNameSpace.add(stringVector)
    dataSpace2.persistentNameSpace.add(integerVector)
  }
  and "a data frame in the temporary name space", {
    df = new DataFrame(dataSpace2)
    dataSpace2.tempNameSpace.assign("table",df)
  }
  when "I add these vectors to the data frame", {
    df.cbind("names",stringVector).cbind("numbers", integerVector).cbind("prices",doubleVector)
  }
  then "the vectors that were persistent, remain persistent", {
    def l = []
    l.addAll(dataSpace2.persistentNameSpace.keySet())
    l.sort().join(", ").shouldBe "__IntegerVector_2, __StringVector_1"
  }
  and "the rest are in the temporary name space", {
    def l = []
    l.addAll(dataSpace2.tempNameSpace.keySet())
    l.sort().join(", ").shouldBe "__DoubleVector_1, table"
  }
}

scenario "using assign as a rename operation", {
  given "an annonymous vector in the persistent name space", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.add(stringVector)
    }
  }
  when "I assign it a name", {
    ensureDoesNotThrow(Exception) {
      dataSpace2.persistentNameSpace.assign("strings", stringVector)
    }
  }
  then "the new name is there and the old is gone", {
    def l = []
    l.addAll(dataSpace2.persistentNameSpace.keySet())
    l.sort().join(", ").shouldBe "strings"
  }
}

//todo scenario "promoting from memory resident to temporary", { see issue #475
//  given "a vector in the RAM name space", {
//    ensureDoesNotThrow(Exception) {
//      dataSpace2.ramNameSpace.add(integerVector)
//    }
//  }
//  when "I assign it to the temp namesapce"
//  then "it is in the temp namespace"
//  and "is gobe from the ram name space"
//  and
// todo promoting from temporary to persistent see issue #475
// todo promoting from memory resident to persistent see issue #475

scenario "garbage collection and disabling operations on close", {
  given "some vectors in the temp namespace and others in the permanent namespace", {
    dataSpace2.tempNameSpace.add(stringVector)
    dataSpace2.persistentNameSpace.assign("ints",integerVector)
    // important leaving the associated double vector not assigned to a name space
  }
  when "I close the write enabled data store", {
    ensureDoesNotThrow(Exception) {
      store2.close()
    }
  }
  then "the data space is considered closed as well", {
    dataSpace2.isClosed().shouldBe true
  }
  and "all vectors associated with it are considered closed", {
    [stringVector, integerVector, doubleVector, longVector, logicalVector].collect { v->
      v.isClosed() ? "closed" : "open"
    }.join(", ").shouldBe("closed, closed, closed, closed, closed")
  }
  and "all the temporary segments are deleted", {
    def list = new File("$location2/segments").listRecursive().findAll{it.endsWith(".tmp")}.sort()
    list.join(", ").shouldBe ""
  }
  and "all the persistent segments are still there", {
    def list = new File("$location2/segments").listRecursive().findAll{!(it.endsWith(".tmp"))}.sort()
    list.size().shouldBe 4
  }
  and "I cannot assign to the data space", {
    ensureThrows(DataSpaceException) {
      dataSpace2.assign("boom",doubleVector)
    }
  }
  and "I cannot get from the data space", {
    ensureThrows(DataSpaceException) {
      dataSpace2.get("ints")
    }
  }
  and "various operations on closed vectors will throw an exception", {
    ensureThrows(Exception) {
      integerVector.get(0)
    }
    ensureThrows(Exception) {
      integerVector.iterator()
    }
  }
}

scenario "assigning items from one data store to another is not allowed", {
  given "a data space based on the first data store", {
    printExceptions {ds = new DataSpace(store, mockMemoryManager)}
  }
  then "attempting to assign a vector from a data space to a data space with a different data store is not allowed", {
    ensureThrows(DataSpaceException) {
      ds.assign("boom", integerVector)
    }
  }
}


