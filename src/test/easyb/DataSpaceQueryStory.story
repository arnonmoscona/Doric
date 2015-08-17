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

//this.class.classLoader.rootLoader.addURL( new URL("file://c:/Users/Arnon/projects/intellitrade/server/test/easyb") )
import com.moscona.dataSpace.DataSpace
import com.moscona.dataSpace.impl.*
import com.moscona.dataSpace.stub.MemoryManager
import com.moscona.dataSpace.stub.DataStore
import com.moscona.dataSpace.impl.query.RangeQuery
import com.moscona.dataSpace.*

import static com.moscona.test.easyb.TestHelper.*

import com.moscona.dataSpace.impl.query.EqualsQuery
import com.moscona.dataSpace.impl.query.InQuery
import com.moscona.dataSpace.impl.query.CompareQuery
import com.moscona.dataSpace.impl.query.TopNQuery
import com.moscona.dataSpace.impl.query.UniqueValueHistogramQuery
import com.moscona.dataSpace.impl.query.RangeHistogramQuery
import com.moscona.util.StringHelper
import com.moscona.dataSpace.impl.query.RunningOLHCSummaryHistogramQuery
import com.moscona.dataSpace.util.CompressedBitMap
import com.moscona.test.easyb.DelegatingIterator

description "unit tests for query scenarios on vectors and data frames"

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
  and "a double vector with 32 values", {
    doubleVector = new DoubleVector(dataSpace);
    (1..32).each{doubleVector.append(it as double)}
    doubleVector.seal()
  }
  and "a long vector with 32 values", {
    longVector = new LongVector(dataSpace);
    (1..32).each{longVector.append(it as long)}
    longVector.seal()
  }
  and "an integer vector with 32 values", {
    integerVector = new IntegerVector(dataSpace);
    (1..32).each{integerVector.append(it as int)}
    integerVector.seal()
  }
  and "a logical vector with 32 values", {
    logicalVector = new LogicalVector(dataSpace);
    def b = true
    (1..32).each {
      logicalVector.append(b as boolean)
      b = !b
    }
    logicalVector.seal()
  }
  and "a string vector with 32 values", {
    stringVector = new StringVector(dataSpace);
    (1..9).each{stringVector.append("0"+it.toString())}
    (10..32).each{stringVector.append(it.toString())}
    stringVector.seal()
  }
  and "a result place holder", { result = null }
  and "a query state object", { queryState = new QueryState() }
}

scenario "range query on double vector", {
  given "a range query of [19..23)", {
    query = new RangeQuery<Numeric<Double>>()
    params = query.createParameterList(IVector.BaseType.DOUBLE).set("from", 19.0).set("to", 23.0)
  }
  when "I query the vector", {
    ensureDoesNotThrow(Exception) {
      result = doubleVector.select(query, params, queryState)
    }
  }
  then "I should get 4 results", {
    result.cardinality().shouldBe 4
  }
  and "I can iterate on them", {
    ensureDoesNotThrow(Exception) {
      iterator = new DelegatingIterator(doubleVector.iterator(result)) 
      l = []
      iterator.each{l<<it}
      l.toString().shouldBe "[19.0, 20.0, 21.0, 22.0]"
    }
  }
}

scenario "a range query with vector resolution", {
  given "a double vector with 33 values, one in the range only if resolution is taken into account", {
    doubleVector = new DoubleVector(dataSpace);
    (1..32).each{doubleVector.append(it as double)}
    doubleVector.append(18.9999999999)
    doubleVector.resolution = 0.01
    doubleVector.seal()
  }
  when "I query the vector", {
    ensureDoesNotThrow(Exception) {
      result = doubleVector.select(query, params, queryState)
    }
  }
  then "I should get 4 results", {
    result.cardinality().shouldBe 5
  }
  and "I can iterate on them", {
    ensureDoesNotThrow(Exception) {
      iterator = new DelegatingIterator(doubleVector.iterator(result))
      l = []
      iterator.each{l<<it}
      l.toString().shouldBe "[19.0, 20.0, 21.0, 22.0, 18.9999999999]"
    }
  }
}

scenario "range query on a data frame", {
  given "a data frame", {
    df = new DataFrame(dataSpace)
    df.cbind("doubleColumn", doubleVector)
    df.cbind("longColumn", longVector)
  }
  and "an intersection set", {
    query1 = new RangeQuery<Numeric<Double>>()
    params1 = query1.createParameterList(IVector.BaseType.DOUBLE).set("from", 19.0).set("to", 23.0)

    query2 = new RangeQuery<Numeric<Long>>()
    params2 = query2.createParameterList(IVector.BaseType.LONG).set("from", 18L).set("to", 20L)

    query = new QueryIntersectionSet(df);
    query.add(query1, params1, "doubleColumn")
    query.add(query2, params2, "longColumn")
  }
  when "I query the data frame", {
    ensureDoesNotThrow(Exception) {
      result = df.select(query)
    }
  }
  then "I should get 2 rows", {
    result.cardinality().shouldBe 2
  }
  and "the query should give us a human readable form as a string", {
    expected = "doubleColumn in [19.00..23.00) AND longColumn in [18..20]"
    actual = "blooper"
    ensureDoesNotThrow(Exception) {
      actual = query.toString()
    }
    actual.shouldBe expected
  }
  and "I should be able to iterate over the rows", {
    rows = []
    iterator = new DelegatingIterator(df.iterator(result))
    iterator.each{
      rows << it.toString()
    }
    rows.size().shouldBe 2
    // FIXME the following is a bad equivalency test
    rows.join(", ").shouldBe "[longColumn:19, doubleColumn:19.0], [longColumn:20, doubleColumn:20.0]"
  }
  and "I should be able to get a subset of the data frame containing only the query results", {
    df2 = null
    ensureDoesNotThrow(Exception) {
      df2 = df.subset(result)
    }
    df2.size().shouldBe 2
    df2.getRow(0).toString().shouldBe "[longColumn:19, doubleColumn:19.0]" // fixme bad equivalency test
    df2.getRow(1).toString().shouldBe "[longColumn:20, doubleColumn:20.0]" // fixme bad equivalency test
  }
}

scenario "equals query on double vector", {
  given "a equals query of value = 30", {
    query = new EqualsQuery<Numeric<Double>>()
    params = query.createParameterList(IVector.BaseType.DOUBLE).set("value", 30.0)
  }
  when "I query the vector", {
    ensureDoesNotThrow(Exception) {
      result = doubleVector.select(query, params, queryState)
    }
  }
  then "I should get 1 results", {
    result.cardinality().shouldBe 1
  }
}

scenario "equals query on string vector", {
  given "a equals query of value = '30'", {
    query = new EqualsQuery<Text>()
    params = query.createParameterList(IVector.BaseType.STRING).set("value", "30")
  }
  when "I query the vector", {
    ensureDoesNotThrow(Exception) {
      result = stringVector.select(query, params, queryState)
    }
  }
  then "I should get 1 results", {
    result.cardinality().shouldBe 1
  }
}

scenario "equals query on logical vector", {
  given "a equals query of value = true", {
    query = new EqualsQuery<Logical>()
    params = query.createParameterList(IVector.BaseType.BOOLEAN).set("value", true)
  }
  when "I query the vector", {
    ensureDoesNotThrow(Exception) {
      result = logicalVector.select(query, params, queryState)
    }
  }
  then "I should get 16 results", {
    result.cardinality().shouldBe 16
  }
}

scenario "unit test of the quick eval of strings for equality", {
  given "several cases", {
    cases = [
      [0,1,5,true,true,false],
      [1,1,5,true,true,null],
      [2,1,5,true,true,null],
      [4,1,5,true,true,null],
      [5,1,5,true,true,null],
      [6,1,5,true,true,false],


      [0,1,5,true,false,false],
      [1,1,5,true,false,null],
      [2,1,5,true,false,null],
      [4,1,5,true,false,null],
      [5,1,5,true,false,false],
      [6,1,5,true,false,false],

      [0,1,5,false,true,false],
      [1,1,5,false,true,false],
      [2,1,5,false,true,null],
      [4,1,5,false,true,null],
      [5,1,5,false,true,null],
      [6,1,5,false,true,false],

      [4,4,4,true,true,true],
      [4,4,4,false,true,true],
      [4,4,4,true,false,true],
    ]
  }
  and "a results list", {
    result = []
  }
  when "I run all the cases", {
    def i= -1
    cases.each {
      i++
      query = new EqualsQuery<Text>()
      params = query.createParameterList(IVector.BaseType.STRING).set("value", it[0] as String)
      r = query.quickEval(it[0].toString(), it[1].toString(), it[2].toString(), it[3], it[4])
      result << "$i: case: $it expected: ${it[5]} got: $r (${r==it[5] ? 'pass' : 'fail'})"
    }
  }
  then "I should get the expected results", {
    failed = result.grep(~/.*(fail).*/).join("\n").trim()
    failed.shouldBe ""
  }
}

scenario "unit test of the quick eval of integer for equality", {
  given "several cases", {
    // test range, segment range, test range closed on left, right  => always testing closed both ends when doing equality
    cases = [
      [0,1,5,true,true,false],
      [1,1,5,true,true,null],
      [2,1,5,true,true,null],
      [4,1,5,true,true,null],
      [5,1,5,true,true,null],
      [6,1,5,true,true,false],

      [4,4,4,true,true,true],
    ]
  }
  and "a results list", {
    result = []
  }
  when "I run all the cases", {
    def i= -1
    cases.each {
      i++
      query = new EqualsQuery<Numeric<Integer>>()
      params = query.createParameterList(IVector.BaseType.INTEGER).set("value", it[0] as Integer) // kind of irrelevant for the test
      r = query.quickEval(it[0] as Long, it[0] as Long, it[1] as Long, it[2] as Long, it[3], it[4])
      result << "$i: case: $it expected: ${it[5]} got: $r (${r==it[5] ? 'pass' : 'fail'})"
    }
  }
  then "I should get the expected results", {
    failed = result.grep(~/.*(fail).*/).join("\n").trim()
    failed.shouldBe ""
  }
}

scenario "unit test of the quick eval of integer for range", {
  given "several cases", {
    // test range, segment range, test range closed on left, right
    cases = [
      [0,1,2,5,true,true,false],
      [0,2,2,5,true,true,null],
      [0,3,2,5,true,true,null],
      [2,3,2,5,true,true,null],
      [2,5,2,5,true,true,null],
      [2,6,2,5,true,true,null],
      [5,6,2,5,true,true,null],
      [6,8,2,5,true,true,false],
      [4,4,4,4,true,true,true],

      [0,1,2,5,false,true,false],
      [0,2,2,5,false,true,null],
      [0,3,2,5,false,true,null],
      [2,3,2,5,false,true,null],
      [2,5,2,5,false,true,null],
      [2,6,2,5,false,true,null],
      [5,6,2,5,false,true,false],
      [6,8,2,5,false,true,false],
      [4,4,4,4,false,true,true],

      [0,1,2,5,true,false,false],
      [0,2,2,5,true,false,false],
      [0,3,2,5,true,false,null],
      [2,3,2,5,true,false,null],
      [2,5,2,5,true,false,null],
      [2,6,2,5,true,false,null],
      [5,6,2,5,true,false,null],
      [6,8,2,5,true,false,false],
      [4,4,4,4,true,false,true],
    ]
  }
  and "a results list", {
    result = []
  }
  when "I run all the cases", {
    def i= -1
    cases.each {
      i++
      query = new EqualsQuery<Numeric<Integer>>()
      params = query.createParameterList(IVector.BaseType.INTEGER).set("value", it[0] as Integer) // kind of irrelevant for the test
      r = query.quickEval(it[0] as Long, it[1] as Long, it[2] as Long, it[3] as Long, it[4], it[5])
      result << "$i: case: $it expected: ${it[6]} got: $r (${r==it[6] ? 'pass' : 'fail'})"
    }
  }
  then "I should get the expected results", {
    failed = result.grep(~/.*(fail).*/).join("\n").trim()
    failed.shouldBe ""
  }
}

scenario "unit test of the quick eval of double for range", {
  given "several cases", {
    // test range, segment range, test range closed on left, right
    cases = [
      [0,1,2,5,true,true,false],
      [0,2,2,5,true,true,null],
      [0,3,2,5,true,true,null],
      [2,3,2,5,true,true,null],
      [2,5,2,5,true,true,null],
      [2,6,2,5,true,true,null],
      [5,6,2,5,true,true,null],
      [6,8,2,5,true,true,false],
      [4,4,4,4,true,true,true],

      [0,1,2,5,false,true,false],
      [0,2,2,5,false,true,null],
      [0,3,2,5,false,true,null],
      [2,3,2,5,false,true,null],
      [2,5,2,5,false,true,null],
      [2,6,2,5,false,true,null],
      [5,6,2,5,false,true,false],
      [6,8,2,5,false,true,false],
      [4,4,4,4,false,true,true],

      [0,1,2,5,true,false,false],
      [0,2,2,5,true,false,false],
      [0,3,2,5,true,false,null],
      [2,3,2,5,true,false,null],
      [2,5,2,5,true,false,null],
      [2,6,2,5,true,false,null],
      [5,6,2,5,true,false,null],
      [6,8,2,5,true,false,false],
      [4,4,4,4,true,false,true],
    ]
  }
  and "a results list", {
    result = []
  }
  when "I run all the cases", {
    def i= -1
    cases.each {
      i++
      query = new EqualsQuery<Numeric<Integer>>()
      params = query.createParameterList(IVector.BaseType.DOUBLE).set("value", it[0] as Double) // kind of irrelevant for the test
      r = query.quickEval(it[0] as Double, it[1] as Double, it[2] as Double, it[3] as Double, it[4], it[5], true, 0.1)
      result << "$i: case: $it expected: ${it[6]} got: $r (${r==it[6] ? 'pass' : 'fail'})"
    }
  }
  then "I should get the expected results", {
    failed = result.grep(~/.*(fail).*/).join("\n").trim()
    failed.shouldBe ""
  }
}

scenario "query on a set of integers", {
  given "a set of 2,3,5,13,9", {
    HashSet set = new HashSet([2,3,5,13,9])
    query = new InQuery<Numeric<Integer>>()
    params = query.createParameterList(IVector.BaseType.INTEGER).set("values", set)
  }
  when "I query", {
    ensureDoesNotThrow(Exception) {
      result = integerVector.subset(query, params, new QueryState())
    }
  }
  then "I should get 5 results", {
    result.size().shouldBe 5
  }
  and "the query should translate to a human readable string", {
    query.toString(params).shouldBe "in [2, 3, 5, 9, 13]"
  }
}

scenario "query on a set of strings", {
  given "a set of 2,3,5,13,9", {
    HashSet set = new HashSet([2,3,5,13,9].collect{it.toString()})
    query = new InQuery<String>()
    params = query.createParameterList(IVector.BaseType.STRING).set("values", set)
  }
  when "I query", {
    ensureDoesNotThrow(Exception) {
      result = stringVector.subset(query, params, new QueryState())
    }
  }
  then "I should get 1 results", {
    result.size().shouldBe 1 // the rest don't match because the vector has 01,02 etc up to 10
  }
  and "the query should translate to a human readable string", {
    query.toString(params).shouldBe "in ['2', '3', '5', '9', '13']" // fixme bad equivalency test
  }
}

scenario "A compare query on doubles", {
  given "several test cases", {
    cases = [
      [">", 31, 1, 32, 32], // op, boundary, count, first, last
      [">=", 31, 2, 31, 32],
      ["<", 3, 2, 1, 2],
      ["<=", 3, 3, 1, 3],
    ]
  }
  and "a results list", {
    result = []
  }
  when "each is executed", {
    ensureDoesNotThrow(Exception) {
      cases.each {
        def query = new CompareQuery()
        def params = query.createParameterList(IVector.BaseType.DOUBLE).set(CompareQuery.COMPARE_TO, it[1]).set(CompareQuery.OPERATOR, it[0])
        def subset = doubleVector.subset(query,params,new QueryState());
        def sizeOK = subset.size()==it[2]
        def firstOK = Math.abs(subset.get(0).value-it[3]) < 0.01
        def lastOK = Math.abs(subset.get(subset.size()-1).value-it[4]) < 0.01
        if (!sizeOK || !firstOK || !lastOK) {
          println "case: $it"
          (0..subset.size()-1).each{num->println("  $num : ${subset.get(num).value}")}
          result << (["case":it, "sizeOK":sizeOK, "size":subset.size(), "firstOK":firstOK, "first":subset.get(0).value, "last":subset.get(subset.size()-1).value, "lastOK":lastOK] as String)
        }
      }
    }
  }
  then "we should get the expected result", {
    result.join("\n").shouldBe ""
  }
}

scenario "one segment quantile calculation - 5 elements", {
  given "a large segment data space", {
    dataSpace = new DataSpace(new DataStore(), mockMemoryManager)
  }
  and "a float vector with 5 elements", {
    vector = new FloatVector(dataSpace);
    (1..5).each{vector.append(it as float)}
  }
  when "I seal the vector", {
    ensureDoesNotThrow(Exception) {
      vector.seal()
    }
  }
  then "the median should be calculated", {
    result = vector.stats.quantiles
    result.form.toString().shouldBe "MEDIAN_ONLY"
    result.getPercentile(0).shouldBeCloseTo(1.0,0.001)
    result.getPercentile(50).shouldBeCloseTo(3.0,0.001)
    result.getPercentile(100).shouldBeCloseTo(5.0,0.001)
  }
  and "other quantiles should be null", {
    [5,10,15,20,25,30,35,40,45,55,60,65,70,75,80,85,90,95].each {q->
      "$q: ${result.getPercentile(q)}".shouldBe "$q: null"
    }
  }
}

scenario "one segment quantile calculation - 15 elements", {
  given "a large segment data space", {
    dataSpace = new DataSpace(new DataStore(), mockMemoryManager)
  }
  and "a float vector with 5 elements", {
    vector = new FloatVector(dataSpace);
    (1..15).each{vector.append(it as float)}
  }
  when "I seal the vector", {
    ensureDoesNotThrow(Exception) {
      vector.seal()
    }
  }
  then "the median should be calculated", {
    result = vector.stats.quantiles
    result.form.toString().shouldBe "FOUR_QUANTILES"
    result.getPercentile(0).shouldBeCloseTo(1.0,0.001)
    result.getPercentile(25).shouldBeCloseTo(4.0,0.001)
    result.getPercentile(50).shouldBeCloseTo(8.0,0.001)
    result.getPercentile(75).shouldBeCloseTo(12.0,0.001)
    result.getPercentile(100).shouldBeCloseTo(15.0,0.001)
  }
  and "other quantiles should be null", {
    [5,10,15,20,30,35,40,45,55,60,65,70,80,85,90,95].each {q->
      "$q: ${result.getPercentile(q)}".shouldBe "$q: null"
    }
  }
}

scenario "one segment quantile calculation - 150 elements", {
  given "a large segment data space", {
    dataSpace = new DataSpace(new DataStore(), mockMemoryManager)
  }
  and "a float vector with 5 elements", {
    vector = new FloatVector(dataSpace);
    (1..150).each{vector.append(it as float)}
  }
  when "I seal the vector", {
    ensureDoesNotThrow(Exception) {
      vector.seal()
    }
  }
  then "the median should be calculated", {
    result = vector.stats.quantiles
    result.form.toString().shouldBe "FIVE_PERCENTILE_BINS"
    result.getPercentile(0).shouldBeCloseTo(1.0,0.001)
    result.getPercentile(5).shouldBeCloseTo(8.0,0.001)
    result.getPercentile(25).shouldBeCloseTo(38.0,0.001)
    result.getPercentile(50).shouldBeCloseTo(75.0,0.001)
    result.getPercentile(75).shouldBeCloseTo(113.0,0.001)
    result.getPercentile(95).shouldBeCloseTo(143.0,0.001)
    result.getPercentile(100).shouldBeCloseTo(150.0,0.001)
  }
  and "none should be null", {
    [0,5,10,15,20,25,30,35,40,45,55,60,65,70,75,80,85,90,95,0].each {q->
      "$q: ${result.getPercentile(q)}".shouldNotBe "$q: null"
    }
  }
}

scenario "topN query on double vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,6,7,8,9,
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as double)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Double>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}

scenario "bottom 12 query on double vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as double)}
    vector.seal()
  }
  and "a bottom 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Double>>(false, 12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}


// scenarios for topn on double

scenario "top 12 query on short double vector (5 elements)", {
  given "a vector with less than 12 values, and some duplicates in the top 12", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      2,3,4
    ]
    values.each{vector.append(it as double)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Double>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 2, 2, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}

// scenarios for topn on float

scenario "topN query on Float vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new FloatVector(dataSpace);
    values = [
      1,2,3,4,5,6,7,8,9,
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Float)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Float>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}

scenario "bottom 12 query on Float vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new FloatVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Float)}
    vector.seal()
  }
  and "a bottom 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Float>>(false, 12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}



scenario "top 12 query on short Float vector (5 elements)", {
  given "a vector with less than 12 values, and some duplicates in the top 12", {
    vector = new FloatVector(dataSpace);
    values = [
      1,2,3,4,5,
      2,3,4
    ]
    values.each{vector.append(it as Float)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Float>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 2, 2, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}


scenario "topN query on String vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new StringVector(dataSpace);
    values = [
      1,2,3,4,5,6,7,8,9,
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      12,13,14,15,16,
      2,3,4
    ].collect{"$it"}
    values.each{vector.append(it as String)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Text>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "13, 14, 15, 16, 2, 3, 4, 5, 6, 7, 8, 9"  // sorted as strings, these are the top
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "13, 14, 15, 16, 2, 3, 4, 5, 6, 7, 8, 9"  // sorted as strings, these are the top
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 2, 2, 2, 3, 3, 3, 2, 1, 1, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}

scenario "bottom 12 query on String vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new StringVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ].collect{"$it"}
    values.each{vector.append(it as String)}
    vector.seal()
  }
  and "a bottom 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Text>(false, 12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 10, 11, 12, 13, 14, 15, 16, 2, 3, 4, 5"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 10, 11, 12, 13, 14, 15, 16, 2, 3, 4, 5"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}



scenario "top 12 query on short String vector (5 elements)", {
  given "a vector with less than 12 values, and some duplicates in the top 12", {
    vector = new StringVector(dataSpace);
    values = [
      1,2,3,4,5,
      2,3,4
    ].collect{"$it"}
    values.each{vector.append(it as String)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Text>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 2, 2, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}

scenario "topN query on Integer vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,6,7,8,9,
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Integer)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Integer>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"  // sorted as Integers, these are the top
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"  // sorted as Integers, these are the top
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}

scenario "bottom 12 query on Integer vector", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Integer)}
    vector.seal()
  }
  and "a bottom 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Integer>>(false, 12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}



scenario "top 12 query on short Integer vector (5 elements)", {
  given "a vector with less than 12 values, and some duplicates in the top 12", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      2,3,4
    ]
    values.each{vector.append(it as Integer)}
    vector.seal()
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Integer>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 2, 2, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}

scenario "unique value histogram query on double", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new UniqueValueHistogramQuery<Numeric<Double>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 16", {
    result.size().shouldBe 16
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
  }
}

scenario "unique value histogram query on string", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new StringVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append("$it")}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new UniqueValueHistogramQuery<Text>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 16", {
    result.size().shouldBe 16
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 10, 11, 12, 13, 14, 15, 16, 2, 3, 4, 5, 6, 7, 8, 9"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 10, 11, 12, 13, 14, 15, 16, 2, 3, 4, 5, 6, 7, 8, 9"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 10, 11, 12, 13, 14, 15, 16, 2, 3, 4, 5, 6, 7, 8, 9"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 2, 1, 1, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
  }
}

scenario "unique value histogram query on string", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Integer)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new UniqueValueHistogramQuery<Numeric<Integer>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 16", {
    result.size().shouldBe 16
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
  }
}

// range histogram query (doubles) ====================================================================================

scenario "creating a range based histogram on a double vector", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 21", {
    result.size().shouldBe 21
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[1.00-1.71), [1.71-2.43), [2.43-3.14), [3.14-3.86), [3.86-4.57), [4.57-5.29), [5.29-6.00), [6.00-6.71), [6.71-7.43), [7.43-8.14), [8.14-8.86), [8.86-9.57), [9.57-10.29), [10.29-11.00), [11.00-11.71), [11.71-12.43), [12.43-13.14), [13.14-13.86), [13.86-14.57), [14.57-15.29), [15.29-16.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.00, 1.71, 2.43, 3.14, 3.86, 4.57, 5.29, 6.00, 6.71, 7.43, 8.14, 8.86, 9.57, 10.29, 11.00, 11.71, 12.43, 13.14, 13.86, 14.57, 15.29"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.71, 2.43, 3.14, 3.86, 4.57, 5.29, 6.00, 6.71, 7.43, 8.14, 8.86, 9.57, 10.29, 11.00, 11.71, 12.43, 13.14, 13.86, 14.57, 15.29, 16.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 0, 3, 2, 0, 1, 1, 1, 0, 1, 1, 0, 1, 2, 2, 0, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"
  }
}

scenario "creating a range based histogram on a double vector (explicit range)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int, 2.0 as double, 8.0 as double)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[2.00-2.60), [2.60-3.20), [3.20-3.80), [3.80-4.40), [4.40-5.00), [5.00-5.60), [5.60-6.20), [6.20-6.80), [6.80-7.40), [7.40-8.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.00, 2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40, 8.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "3, 3, 0, 3, 0, 2, 1, 0, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }
}


scenario "creating a range based histogram on a double vector (specified bin count)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[1.00-2.50), [2.50-4.00), [4.00-5.50), [5.50-7.00), [7.00-8.50), [8.50-10.00), [10.00-11.50), [11.50-13.00), [13.00-14.50), [14.50-16.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.00, 2.50, 4.00, 5.50, 7.00, 8.50, 10.00, 11.50, 13.00, 14.50"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.50, 4.00, 5.50, 7.00, 8.50, 10.00, 11.50, 13.00, 14.50, 16.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5, 3, 5, 1, 2, 1, 2, 2, 4, 4"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }
}


scenario "creating a range based histogram on a double vector (small vector)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[1.00-1.60), [1.60-2.20), [2.20-2.80), [2.80-3.40), [3.40-4.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.00, 1.60, 2.20, 2.80, 3.40"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.60, 2.20, 2.80, 3.40, 4.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 1, 0, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}

// range histogram query (ints) ====================================================================================

scenario "creating a range based histogram on an integer vector", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 16", {
    result.size().shouldBe 16
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
  }
}

scenario "creating a range based histogram on an integer vector with 21 bins", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      41,42,43,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 21", {
    result.size().shouldBe 21
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1-2, 3-4, 5-6, 7-8, 9-10, 11-12, 13-14, 15-16, 17-18, 19-20, 21-22, 23-24, 25-26, 27-28, 29-30, 31-32, 33-34, 35-36, 37-38, 39-40, 41-43"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 43"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "3, 5, 3, 2, 2, 3, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20"
  }
}


scenario "creating a range based histogram on an integer vector (explicit range)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>(10 as int, 2 as long, 8 as long)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 7", {
    result.size().shouldBe 7
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 4, 5, 6, 7, 8"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "2, 3, 4, 5, 6, 7, 8"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "2, 3, 4, 5, 6, 7, 8"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "3, 3, 3, 2, 1, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6"
  }
}


scenario "creating a range based histogram on an integer vector (explicit range with more possible bins)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      41,42,43,4,45,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>(10 as int, 2 as long, 80 as long)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2-8, 9-15, 16-22, 23-29, 30-36, 37-43, 44-50, 51-57, 58-64, 65-80"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "2, 9, 16, 23, 30, 37, 44, 51, 58, 65"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "8, 15, 22, 29, 36, 43, 50, 57, 64, 80"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "12, 11, 2, 0, 0, 3, 1, 0, 0, 0"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }
}


scenario "creating a range based histogram on an integer vector (specified bin count)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>(5 as int)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1-3, 4-6, 7-9, 10-12, 13-16"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 4, 7, 10, 13"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "3, 6, 9, 12, 16"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "8, 6, 3, 4, 8"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
}


scenario "creating a range based histogram on an integer vector (small vector)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new IntegerVector(dataSpace);
    values = [
      1,2,3,4
    ]
    values.each{vector.append(it as int)}
    vector.seal()
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Integer>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 4", {
    result.size().shouldBe 4
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 1, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3"
  }
}



// OLHC histogram query (ints) ====================================================================================

scenario "creating an OLHC histogram for a full float vector", {
  given "a vector with 50 values", {
    vector = new FloatVector(dataSpace);
    values = []
    7.times {i->
      7.times{j-> values << j}
    }
    values << 10
    values.each{vector.append(it as float)}
    vector.seal()
  }
  and "an OLHC histogram query for 10 samples per bin", {
    ensureDoesNotThrow(Exception) {
      query = new RunningOLHCSummaryHistogramQuery(10)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.OLHCHistogram"
  }
  and "its size should be 5", {
    result.size().shouldBe 5
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 10, 10, 10, 10"
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 20, 30, 40, 50"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "0, 0, 0, 0, 0"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "6, 6, 6, 6, 10"
  }
  and "the first should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_FIRST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0.0, 3.0, 6.0, 2.0, 5.0"
  }
  and "the last should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_LAST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.0, 5.0, 1.0, 4.0, 10.0"
  }
  and "the mean should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_MEAN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.4, 3.3, 2.8, 3.0, 4.2"
  }
  and "the stdev should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_STDEV);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.9595917942265426, 1.791647286716892, 2.227105745132009, 1.7320508075688772, 2.749545416973504"
  }
}

scenario "creating an OLHC histogram for a float vector with a selection bitmap", {
  given "a vector with 50 values", {
    vector = new FloatVector(dataSpace);
    values = []
    7.times {i->
      7.times{j-> values << j}
    }
    values << 10
    values.each{vector.append(it as float)}
    vector.seal()
  }
  and "a bitmap selecting 20 of those values", {
    l = [1, 4, 6, 9, 11, 14, 16, 17, 21, 23, 27, 31, 32, 33, 36, 37, 39, 41, 43, 45]
    selection = new CompressedBitMap()
    50.times{i->
      boolean r = l.contains(i)
      selection.add(r)
    }
  }
  and "an OLHC histogram query for 10 samples per bin restricted by the bitmap", {
    ensureDoesNotThrow(Exception) {
      query = new RunningOLHCSummaryHistogramQuery(10,selection)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.OLHCHistogram"
  }
  and "its size should be 2", {
    result.size().shouldBe 2
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 10"
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 20"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "0, 1"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "6, 6"
  }
  and "the first should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_FIRST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 6.0"
  }
  and "the last should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_LAST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.0, 3.0"
  }
  and "the mean should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_MEAN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.4, 3.5"
  }
  and "the stdev should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_STDEV);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.8000000000000003, 1.746424919657298"
  }
}

scenario "getSortedUniqueValues() on a StringVector", {
  given "a string vector", {
    stringVector = new StringVector(dataSpace);
    ["a", "b", "e", "a", "z", "e"].each{stringVector.append(it.toString())}
    stringVector.seal()
  }
  then "the sorted values should be correct", {
    stringVector.sortedUniqueValues.collect{it.toString()}.join(",").shouldBe "a,b,e,z"
  }
}

scenario "getSortedUniqueValues() on a large StringVector", {
  given "a string vector", {
    stringVector = new StringVector(dataSpace);
    5000.times {
      ["a", "b", "e", "a", "z", "e"].each{stringVector.append(it.toString())}
    }
    stringVector.seal()
  }
  then "the sorted values should be correct", {
    stringVector.sortedUniqueValues.collect{it.toString()}.join(",").shouldBe "a,b,e,z"
  }
}

scenario "fast conversion of a double vector with no selection", {
  when "I convert the vector to a list", {
    result = doubleVector.asList()
  }
  then "I should get all the values", {
    result.collect{"$it"}.join(",").shouldBe "1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0,10.0,11.0,12.0,13.0,14.0,15.0,16.0,17.0,18.0,19.0,20.0,21.0,22.0,23.0,24.0,25.0,26.0,27.0,28.0,29.0,30.0,31.0,32.0"
  }
}

scenario "fast conversion of a double vector with selection", {
  given "a selection bitmap", {
    def segment1 = [0,4,9]
    def segment2 = []
    def segment3 = [30,31]
    def positions = segment1+segment2+segment3
    selection = new CompressedBitMap()
    (0..31).each {
      selection.add(positions.contains(it))
    }
  }
  when "I convert the vector to a list using select()", {
    ensureDoesNotThrow(Exception) {
      result = doubleVector.select(selection)
    }
  }
  then "I should get the selected values", {
    result.collect{"$it"}.join(",").shouldBe "1.0,5.0,10.0,31.0,32.0"
  }
}

scenario "fast conversion of a string vector with no selection", {
  when "I convert the vector to a list", {
    result = stringVector.asList()
  }
  then "I should get all the values", {
    result.collect{"$it"}.join(",").shouldBe "01,02,03,04,05,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32"
  }
}

scenario "fast conversion of a string vector with selection", {
  given "a selection bitmap", {
    def segment1 = [0,4,9]
    def segment2 = []
    def segment3 = [30,31]
    def positions = segment1+segment2+segment3
    selection = new CompressedBitMap()
    (0..31).each {
      selection.add(positions.contains(it))
    }
  }
  when "I convert the vector to a list using select()", {
    ensureDoesNotThrow(Exception) {
      result = stringVector.select(selection)
    }
  }
  then "I should get the selected values", {
    result.collect{"$it"}.join(",").shouldBe "01,05,10,31,32"
  }
}

// Transformers with a selection constraint ============================================================================

scenario "RangeHistogramQuery with a selection (double)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      5,5,5,5,
      10,11,12,13,14,15,16,
      15,15,15,15,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      7,7,7,7,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a bitmap selection", {
    selection = [
      1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,1,1,
      1,1,1,1,1,
      0,0,0,0,
      1,1,1
    ]
    bitMap = new CompressedBitMap()
    selection.each{bitMap.add(it==1)}
  }
  and "a range histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int, 2.0 as double, 8.0 as double)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, bitMap, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[2.00-2.60), [2.60-3.20), [3.20-3.80), [3.80-4.40), [4.40-5.00), [5.00-5.60), [5.60-6.20), [6.20-6.80), [6.80-7.40), [7.40-8.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.00, 2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40, 8.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "3, 3, 0, 3, 0, 2, 1, 0, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }
}

scenario "RangeHistogramQuery with an empty selection", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      5,5,5,5,
      10,11,12,13,14,15,16,
      15,15,15,15,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      7,7,7,7,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a bitmap selection", {
    bitMap = new CompressedBitMap()
  }
  and "a range histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int, 2.0 as double, 8.0 as double)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, bitMap, new QueryState())
    }
  }
  then " I should get the a histogram", {
    ensure(result) { isNull }
  }
}

scenario "RangeHistogramQuery with a null selection (double)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a range value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int, 2.0 as double, 8.0 as double)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, null, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[2.00-2.60), [2.60-3.20), [3.20-3.80), [3.80-4.40), [4.40-5.00), [5.00-5.60), [5.60-6.20), [6.20-6.80), [6.80-7.40), [7.40-8.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.00, 2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.60, 3.20, 3.80, 4.40, 5.00, 5.60, 6.20, 6.80, 7.40, 8.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "3, 3, 0, 3, 0, 2, 1, 0, 1, 1"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }
}

scenario "RunningOLHCSummaryHistogramQuery with a selection (float, with the new signatures)", {
  given "a vector with 50 values", {
    vector = new FloatVector(dataSpace);
    values = []
    7.times {i->
      7.times{j-> values << j}
    }
    values << 10
    values.each{vector.append(it as float)}
    vector.seal()
  }
  and "a bitmap selecting 20 of those values", {
    l = [1, 4, 6, 9, 11, 14, 16, 17, 21, 23, 27, 31, 32, 33, 36, 37, 39, 41, 43, 45]
    selection = new CompressedBitMap()
    50.times{i->
      boolean r = l.contains(i)
      selection.add(r)
    }
  }
  and "an OLHC histogram query for 10 samples per bin restricted by the bitmap", {
    ensureDoesNotThrow(Exception) {
      query = new RunningOLHCSummaryHistogramQuery(10)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, selection, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.OLHCHistogram"
  }
  and "its size should be 2", {
    result.size().shouldBe 2
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 10"
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "10, 20"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "0, 1"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "6, 6"
  }
  and "the first should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_FIRST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 6.0"
  }
  and "the last should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_LAST);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.0, 3.0"
  }
  and "the mean should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_MEAN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.4, 3.5"
  }
  and "the stdev should match", {
    binNums = result.get(OLHCHistogram.COL_BIN_STDEV);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.8000000000000003, 1.746424919657298"
  }
}

scenario "TopNQuery with a selection (double vector)", {
  given "a vector with more than 12 values, and some duplicates in the top 12", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,6,7,8,9,
      1,2,3,4,5,
      10,11,12,13,14,15,16,
      12,13,14,15,16,
      2,3,4,
      100,200,300,400,500,600,700,800,900
    ]
    values.each{vector.append(it as double)}
    vector.seal()
  }
  and "a selection", {
    selection = new CompressedBitMap()
    values.each{selection.add(it<100)}
  }
  and "a top 12 query", {
    ensureDoesNotThrow(Exception) {
      query = new TopNQuery<Numeric<Double>>(true,12 as short)
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, selection, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 12", {
    result.size().shouldBe 12
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
  }
}


scenario "UniqueValueHistogramQuery with a selection", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      5,5,5,5,
      10,11,12,13,14,15,16,
      15,15,15,15,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      7,7,7,7,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a bitmap selection", {
    selection = [
      1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,1,1,
      1,1,1,1,1,
      0,0,0,0,
      1,1,1
    ]
    bitMap = new CompressedBitMap()
    selection.each{bitMap.add(it==1)}
  }
  and "a unique value histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new UniqueValueHistogramQuery<Numeric<Double>>()
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, bitMap, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 16", {
    result.size().shouldBe 16
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
  }
}

scenario "RangeHistogramQuery with a selection and descriptive stats (double)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new DoubleVector(dataSpace);
    values = [
      1,2,3,4,5,
      5,5,5,5,
      10,11,12,13,14,15,16,
      15,15,15,15,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      7,7,7,7,
      2,3,4
    ]
    values.each{vector.append(it as Double)}
    vector.seal()
  }
  and "a bitmap selection", {
    selection = [
      1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,1,1,
      1,1,1,1,1,
      0,0,0,0,
      1,1,1
    ]
    bitMap = new CompressedBitMap()
    selection.each{bitMap.add(it==1)}
  }
  and "a range histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Double>>(10 as int, true) // true denotes "get also descriptive stats per bin"
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, bitMap, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "[1.00-2.50), [2.50-4.00), [4.00-5.50), [5.50-7.00), [7.00-8.50), [8.50-10.00), [10.00-11.50), [11.50-13.00), [13.00-14.50), [14.50-16.00]"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "1.00, 2.50, 4.00, 5.50, 7.00, 8.50, 10.00, 11.50, 13.00, 14.50"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as double)}
    list.join(", ").shouldBe "2.50, 4.00, 5.50, 7.00, 8.50, 10.00, 11.50, 13.00, 14.50, 16.00"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "5, 3, 5, 1, 2, 1, 2, 2, 4, 4"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }

  and "the stats min values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MIN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 3.0, 4.0, 6.0, 7.0, 9.0, 10.0, 12.0, 13.0, 15.0"
  }
  and "the stats max values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MAX);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 11.0, 12.0, 14.0, 16.0"
  }
  and "the stats mean values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MEAN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.6, 3.0, 4.4, 6.0, 7.5, 9.0, 10.5, 12.0, 13.5, 15.5"
  }
  and "the stats stdev values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_STDEV);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0.4898979485566355, 0.0, 0.48989794855663327, 0.0, 0.5, 0.0, 0.5, 0.0, 0.5, 0.5"
  }
}

scenario "RangeHistogramQuery with a selection and descriptive stats (long)", {
  given "a vector with more than 12 values, and some duplicates", {
    vector = new LongVector(dataSpace);
    values = [
      1,2,3,4,5,
      5,5,5,5,
      10,11,12,13,14,15,16,
      15,15,15,15,
      1,2,3,4,5,6,7,8,9,
      12,13,14,15,16,
      7,7,7,7,
      2,3,4
    ]
    values.each{vector.append(it as Long)}
    vector.seal()
  }
  and "a bitmap selection", {
    selection = [
      1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,
      0,0,0,0,
      1,1,1,1,1,1,1,1,1,
      1,1,1,1,1,
      0,0,0,0,
      1,1,1
    ]
    bitMap = new CompressedBitMap()
    selection.each{bitMap.add(it==1)}
  }
  and "a range histogram query", {
    ensureDoesNotThrow(Exception) {
      query = new RangeHistogramQuery<Numeric<Long>>(10 as int, true) // true denotes "get also descriptive stats per bin"
    }
  }
  when "I apply the transformation", {
    ensureDoesNotThrow(Exception) {
      result = query.transform(vector, bitMap, new QueryState())
    }
  }
  then " I should get the a histogram", {
    result.getClass().name.shouldBe "com.moscona.dataSpace.Histogram"
  }
  and "its size should be 10", {
    result.size().shouldBe 10
  }
  and "the names should match", {
    names = result.get(Histogram.COL_NAME);
    list = []
    (new DelegatingIterator(names.iterator())).each{list << "$it"}
    // crappy auto-binning for a small range of integers
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10-16"
  }
  and "the min bins should match", {
    bins = result.get(Histogram.COL_BIN_MIN);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10"
  }
  and "the max bins should match", {
    bins = result.get(Histogram.COL_BIN_MAX);
    list = []
    (new DelegatingIterator(bins.iterator())).each{list << StringHelper.prettyPrint(it.value.doubleValue() as long)}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 16"
  }
  and "the counts should match", {
    counts = result.get(Histogram.COL_COUNT);
    list = []
    (new DelegatingIterator(counts.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "2, 3, 3, 3, 2, 1, 1, 1, 1, 12"
  }
  and "the bin numbers should match", {
    binNums = result.get(Histogram.COL_BIN_NUMBER);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0, 1, 2, 3, 4, 5, 6, 7, 8, 9"
  }

  and "the stats min values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MIN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 10"
  }
  and "the stats max values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MAX);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1, 2, 3, 4, 5, 6, 7, 8, 9, 16"
  }
  and "the stats mean values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_MEAN);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 13.416666666666666"
  }
  and "the stats stdev values should match", {
    binNums = result.get(Histogram.COL_BIN_STATS_STDEV);
    list = []
    (new DelegatingIterator(binNums.iterator())).each{list << "$it"}
    list.join(", ").shouldBe "0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.8465433171800352"
  }
}

scenario "iterating over a data frame with specified columns (no selection)", {
  given "a data frame", {
    df = new DataFrame(dataSpace)
    df.cbind("doubleColumn", doubleVector)
    df.cbind("longColumn", longVector)
    df.cbind("stringColumn", stringVector)
  }
  when "I create an iterator for two columns", {
    iterator = df.iterator(["doubleColumn","stringColumn"])
  }
  then "the iterator hasNext() should say true", {
    iterator.hasNext().shouldBe true
  }
  and "getting a row should give us two columns - one double and one string", {
    def row = null
    ensureDoesNotThrow(Exception) {
      row = iterator.next()
    }
    row.size().shouldBe 2
    def keys = row.keySet()
    def sortedKeys = []
    sortedKeys.addAll(keys)
    sortedKeys.sort().join(",").shouldBe "doubleColumn,stringColumn"
  }
}
scenario "iterating over a data frame with specified columns (wtih selection)", {
  given "a data frame", {
    df = new DataFrame(dataSpace)
    df.cbind("doubleColumn", doubleVector)
    df.cbind("longColumn", longVector)
    df.cbind("stringColumn", stringVector)
  }
  and "a selection bitmap", {
    def segment1 = [0,4,9]
    def segment2 = []
    def segment3 = [30,31]
    def positions = segment1+segment2+segment3
    selection = new CompressedBitMap()
    (0..31).each {
      selection.add(positions.contains(it))
    }
  }
  when "I create an iterator for two columns", {
    iterator = df.iterator(selection, ["doubleColumn","stringColumn"])
  }
  then "the iterator hasNext() should say true", {
    iterator.hasNext().shouldBe true
  }
  and "getting a row should give us two columns - one double and one string", {
    def row = null
    ensureDoesNotThrow(Exception) {
      row = iterator.next()
    }
    row.size().shouldBe 2
    def keys = row.keySet()
    def sortedKeys = []
    sortedKeys.addAll(keys)
    sortedKeys.sort().join(",").shouldBe "doubleColumn,stringColumn"
  }
}
// HOLD (fix before release)  tests for estimated quantiles (after we have something real to work with)