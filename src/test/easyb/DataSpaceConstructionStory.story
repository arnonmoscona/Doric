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

import static com.moscona.test.easyb.TestHelper.*
import com.moscona.dataSpace.*
import com.moscona.dataSpace.impl.*
import org.mockito.Mockito

import com.moscona.dataSpace.persistence.IMemoryManaged
import com.moscona.dataSpace.stub.*

import com.moscona.dataSpace.exceptions.DataSpaceException

description "an assortment of unit tests (incomplete) for building data space objects. Queries are tested separately as is persistence and memory management"

before_each "scenario", {
  given "a mock data store", {
    mockDataStore = new DataStore()
  }
  and "a submit counter", {
    submits = 0
  }
  and "a mock memory manager", {
    mockMemoryManager = new MemoryManager()

  }
  and "a data space", {
    dataSpace = new DataSpace(mockDataStore, mockMemoryManager)
  }
  and "a segment size of 10", {
    dataSpace.segmentSize = 10
  }
  and "a scalar", {
    dataItem = new Text("data item")
  }
  and "a result placeholder", { result = null }
}
scenario "testing the memory manager mock", {
  when "I submit two objects", {
    obj1 = Mockito.mock(IMemoryManaged)
    mockMemoryManager.submit(obj1)
    obj2 = Mockito.mock(IMemoryManaged)
    mockMemoryManager.submit(obj2)
  }
  then "the submit counter should increment accordingly", {
    mockMemoryManager.counter.shouldBe 2
  }
}

scenario "creating a small string vector", {
  given "I create a vector of 3 items", {
    vector = null
    printExceptions {
      String[] values = ["1","2","3"]
      vector = new StringVector(values, dataSpace)
    }
  }
  when "I seal the vector", {
    ensureDoesNotThrow(Exception) {
      vector.seal();
    }
  }
  then "the size should be 3", {
    vector.size().shouldBe 3
  }
  and "the contents should text scalars", {
    vector.get(0).getClass().toString().shouldBe "class com.moscona.dataSpace.Text"
  }
  and "the values should be identical to the original", {
    "${vector.get(0)},${vector.get(1)},${vector.get(2)}".shouldBe "1,2,3"
  }
}

scenario "creating a vector that spans 3 segments", {
  given "a vector variable", {
    vector = null
  }
  when "I create a vector with 22 element", {
    ensureDoesNotThrow(Exception) {
      vector = new StringVector(dataSpace)
      (1..22).each {
        vector.append(new Text(it.toString()))
      }
      vector.seal()
    }
  }
  then "its size should be 22", {
    vector.size().shouldBe 22
  }
  and "its values should match what was put in", {
    l = []
    (0..21).each {
      l << vector.get(it).toString()
    }
    l.join(",").shouldBe "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
  }
}

scenario "appending one string vector to another", {
  given "and open string vector", {
    vector1 = new StringVector(dataSpace)
    (1..5).each { vector1.append(new Text(it.toString())) }
  }
  and "a sealed vector", {
    vector2 = new StringVector(dataSpace)
    (6..22).each { vector2.append(new Text(it.toString())) }
    vector2.seal()
  }
  when "I append the second to the first and seal", {
    ensureDoesNotThrow(Exception) {
      vector1.append(vector2);
      vector1.seal()
    }
  }
  then "I should get the concatendated vector",{
    l = []
    (0..21).each {
      l << vector1.get(it).toString()
    }
    l.join(",").shouldBe "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
  }
}

scenario "getting the unique values for a string vector", {
  given "a vector with repeating strings", {
    String[] values = ["2","1","11","1","2","1","11","1","2","1","11","1","2","1","11","1","2","1","11","1","2","1","11","1","2"]
    vector = new StringVector(values, dataSpace)
    vector.seal()
  }
  then "I should be able to get a unique, sorted list of its values", {
    def unique = vector.getSortedUniqueValues()
    unique.toString().shouldBe "[1, 11, 2]"
  }
}

scenario "getting unique values for a double vector (with specified resolution)", {
  given "a vector with resolution of 0.1", {
    vector = new DoubleVector(dataSpace)
    vector.resolution = 0.1
  }
  and "100 different values in it, but only 10 unique considering the resolution", {
    1.upto(10) { loop ->
      1.upto(10) { i->
        def element = Math.random() / 100.0 + i + 0.5
        vector.append(element);
      }
    }
    vector.seal();
  }
  when "I ask for unique values on this vector", {
    ensureDoesNotThrow(Exception) {
      result = vector.sortedUniqueValues
    }
  }
  then "I should only get 10 values", {
    result.size().shouldBe 10
  }
}

// Numeric -------------------------------------------------------------------------------------------------------------

scenario "creating a long vector that spans 3 segments", {
  given "a vector variable", {
    vector = null
  }
  when "I create a vector with 22 element", {
    ensureDoesNotThrow(Exception) {
      vector = new LongVector(dataSpace)
      (1..22).each {
        vector.append(it)
      }
      vector.seal()
    }
  }
  then "its size should be 22", {
    vector.size().shouldBe 22
  }
  and "its values should match what was put in", {
    l = []
    (0..21).each {
      l << vector.get(it).toString()
    }
    l.join(",").shouldBe "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
  }
  and "its stats should be correct", {
    stats = vector.stats.descriptiveStats
    ensureFloatClose(stats.sum(), 253.0, 0.00001)
    ensureFloatClose(stats.mean(), 11.5, 0.00001)
    ensureFloatClose(stats.variance(), 40.25, 0.00001)
    ensureFloatClose(stats.stdev(), 6.34428877, 0.0000001)
  }
}

// Data space assignment and name spaces -------------------------------------------------------------------------------

scenario "putting stuff in a data space", {
  when "I put a data item in the data space", {
    ensureDoesNotThrow(Exception) {
      dataSpace.assign("var1", dataItem)
    }
  }
  then "it should end up in the default name space for the data space", {
    dataItem.nameSpace.name.shouldBe "temp"
  }
  and "its name should reflect this", {
    dataItem.name.shouldBe "temp.var1"
  }
  and "I should be able to get it with a simple name", {
    dataSpace.get("var1").value.shouldBe "data item"
  }
  and "I should be able to get it with a fully qualified name", {
    dataSpace.get("temp.var1").value.shouldBe "data item"
  }
}

scenario "putting a data item in an explicit namespace", {
  when "I put a data item in the memory name space", {
    dataSpace.assign("memory.var2",dataItem)
  }
  then "I should be able to get it back using the fully qualified name", {
    dataSpace.get("memory.var2").value.shouldBe "data item"
  }
  and "I should not be able to get it back using the simple name", {
    ensureThrows(DataSpaceException) {
      dataSpace.get("var2")
    }
  }
  and "the name of the item should reflect its namespace correctly", {
    dataItem.name.shouldBe "memory.var2"
  }
  and "the item should now be associated with the correct namespace", {
    dataItem.nameSpace.name.shouldBe "memory"
  }
}

scenario "moving a data item from one namespace to another", {
  when "I put a data item in the default name space", {
    dataSpace.assign("flip", dataItem)
  }
  and "then assign it to the persistent name space", {
    dataSpace.assign("persistent.flop", dataItem)
  }
  then "I should be able to get it back using the new name", {
    dataSpace.get("persistent.flop").value.shouldBe "data item"
  }
  and "I should not be able to get it back from the old space", {
    ensureThrows(DataSpaceException) {
      dataSpace.get("flip")
    }
  }
  and "the name of the item should reflect its namespace correctly", {
    dataItem.name.shouldBe "persistent.flop"
  }
  and "the item should now be associated with the correct namespace", {
    dataItem.nameSpace.name.shouldBe "persistent"
  }
}


