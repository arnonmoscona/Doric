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
import com.moscona.dataSpace.SimpleMemoryManager
import java.lang.ref.WeakReference
import com.moscona.dataSpace.stub.MockMemoryManaged
import com.moscona.dataSpace.persistence.PersistenceStatus
import com.moscona.dataSpace.exceptions.DataSpaceException

before_each "scenario", {
  given "a simple memory manager with 1000 bytes of space", {
    mm = new SimpleMemoryManager(1000)
  }
  and "5 objects of size 250", {
    medium = []
    5.times{ medium << new MockMemoryManaged(250,mm)}
  }
  and "5 objects of size 100", {
    small = []
    5.times{ small << new MockMemoryManaged(100,mm)}
  }
  and "5 objects of size 400", {
    large = []
    5.times{ large << new MockMemoryManaged(400,mm)}
  }
  and "a list of all the objects", {
    all = small+medium+large
  }
  and "a result placeholder", {
    result = null
  }
}

if(shouldRunUnsafeScenarios()) scenario "verifying the GC reliably causes weak references to null out", {
  given "an object reference and a weak reference to it", {
    Integer i = 123
    ref = new WeakReference<Integer>(i)
  }
  when "I do a GC", {
    System.gc()
  }
  then "the weak reference is nulled out", {
    ensure(ref.get()){isNull}
  }
}

scenario "submitting it all", {
  then "submitting all objects should cause no exceptions", {
    result = []
    ensureDoesNotThrow(Exception) {
      all.each { item->
        result << mm.submit(item)
      }
    }
  }
  and "they should all have unique IDs", {
    l = []
    all.each { l << it.memoryManagerId }
    l.unique().size().shouldBe 15
  }
  and "their IDs should match waht was returned by submit for each", {
    ids = all.collect{item->item.memoryManagerId}
    ids.join(",").shouldBe result.join(",")
  }
  and "at least some objects should be in a swapped in state", {
    all.inject(false){res,item-> res||(item.persistenceStatus == PersistenceStatus.SWAPPED_IN)}.shouldBe true
  }
  and "at least some objects should be in a swapped out state", {
    all.inject(false){res,item-> res||(item.persistenceStatus == PersistenceStatus.SWAPPED_OUT)}.shouldBe true
  }
  and "at least no objects should be in a not persisted state", {
    all.inject(false){res,item-> res||(item.persistenceStatus == PersistenceStatus.NOT_PERSISTED)}.shouldBe false
  }
  and "the total memory utilization of swapped in objects should be below the max", {
    all.inject(0){res,item-> (item.persistenceStatus == PersistenceStatus.SWAPPED_IN) ? res + item.sizeInBytes() : res}.shouldBeLessThan 1001
  }
  and "the last two objects should be swapped in", {
    [-1,-2].each{i->(all[i].persistenceStatus == PersistenceStatus.SWAPPED_IN).shouldBe true}
  }
  and "the first 13 objects should be swapped out", {
    (0..12).each{i->(all[i].persistenceStatus == PersistenceStatus.SWAPPED_OUT).shouldBe true}
  }
}

scenario "submit something that's already there", {
  given "that I submitted an object", {
    small.each{mm.submit(it)}
  }
  when "I submit it again (should not really do that)", {
    result = small[0].memoryManagerId
    mm.submit(small[0])
  }
  then "it does not change its memory manager id", {
    small[0].memoryManagerId.shouldBe result
  }
  and "the churn rate should be 0", {
    ensureFloatClose(mm.churnRate, 0.0, 0.001)
  }
  and "submitting an object with a non-existing memory manager id would result in an exception", {
    medium[0].memoryManagerId = 10000
    ensureThrows(DataSpaceException) {
      mm.submit(medium[0])
    }
  }
  and "submitting an object with the same memory manager ID as a different object would also result in an exception", {
    medium[1].memoryManagerId = result
    ensureThrows(DataSpaceException) {
      mm.submit(medium[1])
    }
  }
}

scenario "simple require and release", {
  given "that all objects are submitted", {
    all.each{mm.submit(it)}
    utilization = mm.totalUtilization
  }
  when "I require a swapped out object", {
    ensureDoesNotThrow(Exception) {
      mm.require(small[0].memoryManagerId)
    }
  }
  then "it is swapped in", {
    (small[0].persistenceStatus == PersistenceStatus.SWAPPED_IN).shouldBe true
  }
  and "the oldest LRU member should be swapped in", {
    (all[-2].persistenceStatus == PersistenceStatus.SWAPPED_IN).shouldBe true
  }
  and "memory utilization should have incremented by 100", {
    (mm.totalUtilization - utilization).shouldBe 100
  }
  and "when I release it it is still swapped in", {
    ensureDoesNotThrow(Exception) {
      mm.release(small[0].memoryManagerId)
    }
    (small[0].persistenceStatus == PersistenceStatus.SWAPPED_IN).shouldBe true
  }
  and "memory utilization should remain the same", {
    (mm.totalUtilization - utilization).shouldBe 100
  }
  and "when I submit a very large object, my small object remains swapped in", {
    mm.submit(new MockMemoryManaged(mm.slack+1,mm))
    (small[0].persistenceStatus == PersistenceStatus.SWAPPED_IN).shouldBe true
  }
  and "the oldest LRU member should be swapped out", {
    (all[-2].persistenceStatus == PersistenceStatus.SWAPPED_OUT).shouldBe true
  }
}

scenario "submit and require when exceeded max utilization", {
  given "that all objects are submitted", {
    all.each{mm.submit(it)}
  }
  when "I require all of them", {
    ensureDoesNotThrow(Exception) {
      all.each{mm.require(it.memoryManagerId)}
    }
  }
  then "the utilization should exceed max", {
    mm.slack.shouldBeLessThan 0
  }
  and "all of them are swapped in", {
    all.inject(true){res,item-> res && (item.persistenceStatus == PersistenceStatus.SWAPPED_IN)}.shouldBe true
  }
  and "releasing them returns the utilization to normal", {
    ensureDoesNotThrow(Exception) {
      all.each{mm.release(it.memoryManagerId)}
    }
    mm.slack.shouldBeGreaterThan 0
  }
  and "the high water mark is the total size of all the objects", {
    total = all.inject(0){res,item-> res+item.sizeInBytes()}
    mm.highWaterMark.shouldBe total
  }
  and "the churn rate should be 0.9285", {
    ensureFloatClose(mm.churnRate, 0.9285, 0.001)
  }
  and "the churn rate by volume should be 0.88059", {
    ensureFloatClose(mm.churnRateByVolume, 0.88059, 0.001)
  }
  and "the average IO cost of requires should be positive", {
    mm.requireAvgIoCost.shouldBeGreaterThan 0.0
  }
}

scenario "submit in swapped out state (as in add vector segment after loading vector from disk, getting a new ID but not swapping in)", {
  when "I submit an object is a swapped out state", {
    mm.submit(small[0])
    result = mm.slack
    small[2].persistenceStatus = PersistenceStatus.SWAPPED_OUT
    mm.submit(small[2])
  }
  then "it remains swapped out", {
    (small[2].persistenceStatus == PersistenceStatus.SWAPPED_OUT).shouldBe true
  }
  and "memory utilization does not increase", {
    mm.slack.shouldBe result
  }
}

scenario "onSwappedOut", {
  given "that a few of objects are submitted", {
    (0..2).each{ mm.submit(small[it])}
    totalUtilization = mm.totalUtilization
  }
  when "I notify the memory manager that one of them was swapped out", {
    ensureDoesNotThrow(Exception) {
      mm.onSwappedOut(small[0])
    }
  }
  then "memory utilization descreases", {
    mm.totalUtilization.shouldBe totalUtilization-100
  }
  and "if I notify again on the same one, then memory utilization does not decrease further", {
    ensureDoesNotThrow(Exception) {
      mm.onSwappedOut(small[0])
    }
    mm.totalUtilization.shouldBe totalUtilization-100
  }
}

scenario "rapid-fire require/release on same object with minimal overhead", {
  given "that I submit a few objects", {
    all.each{mm.submit(it)}
  }
  and "I require some object", {
    mm.require(small[0].memoryManagerId)
    result = mm.requireCounter
    totalUtilization = mm.totalUtilization
  }
  when "require/release the another object several times in succession", {
    10.times {
      mm.require(medium[0].memoryManagerId)
      mm.release(medium[0].memoryManagerId)
    }
  }
  then "the require count should increment by one", {
    mm.requireCounter.shouldBe result+1
  }
  and "the other object should still be in a required state", {
    mm.isRequired(medium[0].memoryManagerId)
  }
  and "the memory utilization does not drop", {
    mm.totalUtilization.shouldBe totalUtilization+250
  }
  and "when I release a different one than the one that was rapid-fire", {
    mm.require(large[0].memoryManagerId)
    mm.release(large[0].memoryManagerId)
  }
  then "both objects should be released", {
    def smallRequired = mm.isRequired(small[0].memoryManagerId)
    def mediumRequired = mm.isRequired(medium[0].memoryManagerId)
    def largeRequired = mm.isRequired(large[0].memoryManagerId)
    def result = "smallRequired:$smallRequired, mediumRequired:$mediumRequired, largeRequired:$largeRequired".toString()
    result.shouldBe "smallRequired:true, mediumRequired:false, largeRequired:true" // now the large one is assumed rapid-fire until something else comes along
  }
}
scenario "track check out time on records and average check out time", {
  given "that all are submitted", {
    all.each{mm.submit(it)}
  }
  and "that all are required and released a few  times", {
    10.times{all.each{obj->
      mm.require(obj.memoryManagerId)
      mm.release(obj.memoryManagerId)
    }}
  }
  when "I require a few objects, I wait 200ms, add a few more, and request a list of outliers", {
    mm.require(medium[0].memoryManagerId)
    mm.require(medium[1].memoryManagerId)
    mm.require(medium[2].memoryManagerId)
    sleep(200)
    mm.require(small[0].memoryManagerId)
    mm.require(small[1].memoryManagerId)
    mm.require(small[2].memoryManagerId)
    result = mm.getOldRequiredIds(10.0,50)
  }

  // FIXME the following is a time sensitive test
  /**
   * // Fail count: 4
   *
   * FAILURE Scenarios run: 8, Failures: 1, Pending: 0, Time elapsed: 4.933 sec
   * 	scenario "track check out time on records and average check out time"
   * 	step "the objects that were sitting there for a long while are flagged as outliers" -- Expected '0,5,6,7' to equal '5,6,7'
   * expected: 5,6,7
   *           |
   *      got: 0,5,6,7
   */
  then "the objects that were sitting there for a long while are flagged as outliers", {
    ids = result.collect{it.id}.sort().join(",")
    expected = [0,1,2].collect{medium[it].memoryManagerId}.join(",")
    ids.shouldBe expected
  }
}
