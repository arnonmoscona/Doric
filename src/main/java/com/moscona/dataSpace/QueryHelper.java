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

package com.moscona.dataSpace;

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.*;
import com.moscona.dataSpace.impl.query.*;
import com.moscona.dataSpace.impl.segment.AbstractVectorSegment;
import com.moscona.dataSpace.persistence.IMemoryManager;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.math.statistics.RangeHistogram;

import java.io.File;
import java.util.HashSet;
import java.util.Map;

/**
 * Created: 2/4/11 8:53 AM
 * By: Arnon Moscona
 * A class that makes it easier to query vectors and data frames.
 * This class is not comprehensive, but is rather built incrementally as the need arises for common query patterns
 */
public class QueryHelper {
    private QueryDebugger queryDebugger = null;

    public QueryHelper() {
        queryDebugger = new NullQueryDebugger();
    }

    public QueryHelper(QueryDebugger queryDebugger) {
        this.queryDebugger = queryDebugger;
    }
    // equals ----------------------------------------------------------------------------------------------------------

    public IBitMap vectorEquals(StringVector vector, String value, final IBitMap intersectWith) throws DataSpaceException {
        final EqualsQuery<Text> query = new EqualsQuery<Text>();
        IQueryParameterList params = query.createParameterList(vector.getBaseType());
        params.set(EqualsQuery.VALUE, value);
        QueryState queryState = new QueryState();

//        long start = System.currentTimeMillis();
//        IBitMap result = query.apply(params, vector, queryState, intersectWith);
//        if (queryDebugger!=null) {
//            queryDebugger.debugQuery("vectorEquals<Text>", params, vector, queryState, result, start);
//        }
//        return result;
        return queryDebugger.perform(new QueryApplyOperation() {
            @Override
            @SuppressWarnings({"unchecked"})
            public IBitMap apply(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState) throws DataSpaceException {
                return query.apply(params, vector, queryState, intersectWith);
            }
        }, "vectorEquals<Text>", params, vector, queryState);
    }

    public IBitMap vectorEquals(IntegerVector vector, int value, final IBitMap intersectWith) throws DataSpaceException {
        final EqualsQuery<Numeric<Integer>> query = new EqualsQuery<Numeric<Integer>>();
        IQueryParameterList params = query.createParameterList(vector.getBaseType());
        params.set(EqualsQuery.VALUE, value);
        QueryState queryState = new QueryState();

//        long start = System.currentTimeMillis();
//        IBitMap result = query.apply(params, vector, queryState, intersectWith);
//        if (queryDebugger!=null) {
//            queryDebugger.debugQuery("vectorEquals<Text>", params, vector, queryState, result, start);
//        }
//        return result;
        return queryDebugger.perform(new QueryApplyOperation() {
            @Override
            @SuppressWarnings({"unchecked"})
            public IBitMap apply(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState) throws DataSpaceException {
                return query.apply(params, vector, queryState, intersectWith);
            }
        }, "vectorEquals<Numeric<Integer>>", params, vector, queryState);
    }

    public IBitMap columnEquals(DataFrame df, String columnName, String value, IBitMap intersectWith) throws DataSpaceException, InvalidArgumentException {
        IVector vector = df.get(columnName);
        if (vector.getClass() != StringVector.class) {
            throw new InvalidArgumentException("The column '"+columnName+"' of the provided data frame is not a string vector");
        }
        return vectorEquals((StringVector)vector, value, intersectWith);   // HOLD (fix before release)  support all column types
    }

    // range queries ---------------------------------------------------------------------------------------------------

    @SuppressWarnings({"unchecked"})
    public<T extends Number> IBitMap columnInRange(DataFrame source, String columnName, int from, int to, final IBitMap intersectWith) throws DataSpaceException {
        IVector<Numeric<T>> vector = (IVector<Numeric<T>>)(source.get(columnName)); // unchecked cast
        final RangeQuery<Numeric<T>> query = new RangeQuery<Numeric<T>>();
        IQueryParameterList params = query.createParameterList(vector.getBaseType());
        params.set(RangeQuery.FROM, from)
                .set(RangeQuery.TO, to)
                .set(RangeQuery.LEFT_CLOSED, true)
                .set(RangeQuery.RIGHT_CLOSED, true);
        final QueryState queryState = new QueryState();
//        long start = System.currentTimeMillis();
//        IBitMap result = query.apply(params, vector, queryState,intersectWith);
//        if (queryDebugger!=null) {
//            queryDebugger.debugQuery("columnInRange<Numeric>", params, vector, queryState, result, start);
//        }
//        return result;
        return queryDebugger.perform(new QueryApplyOperation() {
            @Override
            public IBitMap apply(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState) throws DataSpaceException {
                return query.apply(params, vector, queryState, intersectWith);
            }
        }, "columnInRange<Numeric>", params, vector, queryState);
    }


    // Native array conversion -----------------------------------------------------------------------------------------

    public<T extends Number> double[] asDoubles(IVector<Numeric<T>> vector, IBitMap selection) throws DataSpaceException {
        // HOLD (fix before release)  this is not an efficient conversion
        double[] retval = new double[selection.cardinality()];
        IVectorIterator<Numeric<T>> iterator = vector.iterator(selection);
        int index = 0;
        while (iterator.hasNext()) {
            retval[index++] = iterator.next().getDoubleValue();
        }
        return retval;
    }

    public<T extends Number> double[] asDoubles(AbstractVector<Numeric<T>> vector) throws DataSpaceException {
        // HOLD (fix before release)  this is not an efficient conversion
        double[] retval = new double[vector.size()];
        IVectorIterator<Numeric<T>> iterator = vector.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            retval[index++] = iterator.next().getDoubleValue();
        }
        return retval;
    }

    /**
     * Given a data space histogram creates a RangeHistogram object to match the type
     * @param histogram
     * @return
     */
    @SuppressWarnings({"unchecked"})
    public RangeHistogram<?> createRangeHistogram(Histogram histogram) throws DataSpaceException, InvalidStateException, InvalidArgumentException {
        RangeHistogram<?> retval = null;
        IVector.BaseType baseType = histogram.getBaseType();
        switch(baseType) {
            case BYTE:
                retval = new RangeHistogram<Byte>();
                break;
            case SHORT:
                retval = new RangeHistogram<Short>();
                break;
            case INTEGER:
                retval = new RangeHistogram<Integer>();
                break;
            case LONG:
                retval = new RangeHistogram<Long>();
                break;
            case FLOAT:
                retval = new RangeHistogram<Float>();
                break;
            case DOUBLE:
                retval = new RangeHistogram<Double>();
                break;
            case STRING:
                retval = new RangeHistogram<String>();
                break;  
            case BOOLEAN:
                retval = new RangeHistogram<Boolean>();
                break;
        }
        
        // populate from the histogram (type erasures suck!)
        IVectorIterator<Map<String,IScalar>> iterator = histogram.iterator();
        while (iterator.hasNext()) {
            Map<String,IScalar> row = iterator.next();
            switch(baseType) {
                case BYTE:
                    ((RangeHistogram<Byte>) retval).addBin(getByte(row, Histogram.COL_BIN_MIN), getByte(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case SHORT:
                    ((RangeHistogram<Short>) retval).addBin(getShort(row, Histogram.COL_BIN_MIN), getShort(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case INTEGER:
                    ((RangeHistogram<Integer>) retval).addBin(getInteger(row, Histogram.COL_BIN_MIN), getInteger(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case LONG:
                    ((RangeHistogram<Long>) retval).addBin(getLong(row, Histogram.COL_BIN_MIN), getLong(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case FLOAT:
                    ((RangeHistogram<Float>) retval).addBin(getFloat(row, Histogram.COL_BIN_MIN), getFloat(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case DOUBLE:
                    ((RangeHistogram<Double>) retval).addBin(getDouble(row, Histogram.COL_BIN_MIN), getDouble(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
                case STRING:
                    ((RangeHistogram<String>) retval).addBin(getString(row, Histogram.COL_BIN_MIN), getString(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;  
                case BOOLEAN:
                    ((RangeHistogram<Boolean>) retval).addBin(getBoolean(row, Histogram.COL_BIN_MIN), getBoolean(row, Histogram.COL_BIN_MAX), getInteger(row, Histogram.COL_COUNT));
                    break;
            }
        }

        return retval;
    }

    public Byte getByte(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).byteValue();
    }

    public Short getShort(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).shortValue();
    }

    public Integer getInteger(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).intValue();
    }

    public Long getLong(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).longValue();
    }

    public Float getFloat(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).floatValue();
    }

    public Double getDouble(Map<String, IScalar> row, String key) {
        return ((Number) row.get(key).getValue()).doubleValue();
    }

    public Boolean getBoolean(Map<String, IScalar> row, String key) {
        return (Boolean) row.get(key).getValue();
    }

    public String getString(Map<String, IScalar> row, String key) {
        return (String) row.get(key).getValue();
    }

    @SuppressWarnings({"unchecked"})
    public Histogram getHistogram(IVector<?> vector, IBitMap selection, int binCount, IQueryState queryState) throws DataSpaceException, InvalidArgumentException {
        RangeHistogramQuery query = null;
        switch(vector.getBaseType()) {
            case DOUBLE:
                query = new RangeHistogramQuery<Numeric<Double>>(binCount);
                break;
            case FLOAT:
                query = new RangeHistogramQuery<Numeric<Float>>(binCount);
                break;
            case BYTE:
                query = new RangeHistogramQuery<Numeric<Byte>>(binCount);
                break;
            case SHORT:
                query = new RangeHistogramQuery<Numeric<Short>>(binCount);
                break;
            case INTEGER:
                query = new RangeHistogramQuery<Numeric<Integer>>(binCount);
                break;
            case LONG:
                query = new RangeHistogramQuery<Numeric<Long>>(binCount);
                break;
            default:
                throw new InvalidArgumentException("Cannot create a histogram query for vector of "+vector.getBaseType());
        }
        return (Histogram) query.transform(vector, selection, queryState); // unchecked
    }

    /**
     * Creates a running summary of OLHCHistogram=>DataFrame where the highs are created from the highs,
     * the lows from the lows, the opens from the opens and the close from the close
     * @param ds
     * @param source a source data frame that uses the columns binOpen, binClose, binMin, binMax
     * @param binSize
     * @return
     */
    @SuppressWarnings({"unchecked"})
    public DataFrame summarizeOLHCHistogram(DataSpace ds, DataFrame source, int binSize) throws DataSpaceException, InvalidArgumentException {
        RunningOLHCSummaryHistogramQuery openQuery = new RunningOLHCSummaryHistogramQuery(binSize, new String[]{OLHCHistogram.COL_BIN_OPEN});
        FloatVector openVector = (FloatVector) ((OLHCHistogram) openQuery.transform((source.get(OLHCHistogram.COL_BIN_OPEN)), new QueryState())).get(OLHCHistogram.COL_BIN_OPEN); // unchecked?
        RunningOLHCSummaryHistogramQuery lowQuery = new RunningOLHCSummaryHistogramQuery(binSize, new String[]{OLHCHistogram.COL_BIN_LOW});
        DoubleVector lowVector = (DoubleVector) ((OLHCHistogram) lowQuery.transform((source.get(OLHCHistogram.COL_BIN_LOW)), new QueryState())).get(OLHCHistogram.COL_BIN_LOW); // unchecked?
        RunningOLHCSummaryHistogramQuery highQuery = new RunningOLHCSummaryHistogramQuery(binSize, new String[]{OLHCHistogram.COL_BIN_HIGH});
        DoubleVector highVector = (DoubleVector) ((OLHCHistogram) highQuery.transform((source.get(OLHCHistogram.COL_BIN_HIGH)), new QueryState())).get(OLHCHistogram.COL_BIN_HIGH); // unchecked?
        RunningOLHCSummaryHistogramQuery closeQuery = new RunningOLHCSummaryHistogramQuery(binSize, new String[]{OLHCHistogram.COL_BIN_CLOSE});
        FloatVector closeVector = (FloatVector) ((OLHCHistogram) closeQuery.transform((source.get(OLHCHistogram.COL_BIN_CLOSE)), new QueryState())).get(OLHCHistogram.COL_BIN_CLOSE); // unchecked?

        DataFrame df = new DataFrame(ds);
        df.cbind(OLHCHistogram.COL_BIN_OPEN, openVector)
                .cbind(OLHCHistogram.COL_BIN_LOW, lowVector)
                .cbind(OLHCHistogram.COL_BIN_HIGH, highVector)
                .cbind(OLHCHistogram.COL_BIN_CLOSE, closeVector);
        return df;
    }

    @SuppressWarnings({"unchecked"})
    public Histogram topN(short n, boolean isTop, IVector<?> vector, IQueryState queryState, IBitMap filter) throws InvalidArgumentException, DataSpaceException {
        TopNQuery query = null;
        switch (vector.getBaseType()) {
            case DOUBLE:
                return new TopNQuery<Numeric<Double>>(isTop, n).transform((IVector<Numeric<Double>>) vector, filter, queryState); // unchecked cast
            case FLOAT:
                return new TopNQuery<Numeric<Float>>(isTop, n).transform((IVector<Numeric<Float>>) vector, filter,  queryState); // unchecked cast
            case BYTE:
                return new TopNQuery<Numeric<Byte>>(isTop, n).transform((IVector<Numeric<Byte>>) vector, filter,  queryState); // unchecked cast
            case SHORT:
                return new TopNQuery<Numeric<Short>>(isTop, n).transform((IVector<Numeric<Short>>) vector, filter,  queryState); // unchecked cast
            case INTEGER:
                return new TopNQuery<Numeric<Integer>>(isTop, n).transform((IVector<Numeric<Integer>>) vector, filter,  queryState); // unchecked cast
            case LONG:
                return new TopNQuery<Numeric<Long>>(isTop, n).transform((IVector<Numeric<Long>>) vector, filter,  queryState); // unchecked cast
            case STRING:
                return new TopNQuery<Text>(isTop, n).transform((IVector<Text>) vector, filter,  queryState); // unchecked cast
            default:
                throw new InvalidArgumentException("Cannot create a top N query for vector of "+vector.getBaseType());
        }
    }

    @SuppressWarnings({"unchecked"})
    public IBitMap inQuery(IVector vector, HashSet<Number> values, QueryState queryState) throws InvalidArgumentException, DataSpaceException {
        InQuery query = null;
        switch (vector.getBaseType()) {
            case DOUBLE:
                query = new InQuery<Numeric<Double>>();
                break;
            case FLOAT:
                query = new InQuery<Numeric<Float>>();
                break;
            case BYTE:
                query = new InQuery<Numeric<Byte>>();
                break;
            case SHORT:
                query = new InQuery<Numeric<Short>>();
                break;
            case INTEGER:
                query = new InQuery<Numeric<Integer>>();
                break;
            case LONG:
                query = new InQuery<Numeric<Long>>();
                break;
            case STRING:
                query = new InQuery<Text>();
                break;
            default:
                throw new InvalidArgumentException("Cannot create a top N query for vector of "+vector.getBaseType());
        }
        IQueryParameterList parameterList = query.createParameterList(vector.getBaseType()).set(InQuery.VALUES, values);
        return query.apply(parameterList, vector, queryState); // unchecked cast
    }

    @SuppressWarnings({"unchecked", "ReuseOfLocalVariable", "OverlyLongMethod"})
    public IBitMap lessThanOrEqualsQuery(Number limit, IVector vector, QueryState queryState, IBitMap filter) throws InvalidArgumentException, DataSpaceException {
        if (! vector.isNumeric()) {
            throw new InvalidArgumentException("Cannot create a numeric range query for vector of "+vector.getBaseType());            
        }
        Number min = (Number) vector.getStats().getDescriptiveStats().getMin();
        RangeQuery query = null;
        IQueryParameterList params = null;
        switch (vector.getBaseType()) {
            case DOUBLE:
                query = new RangeQuery<Numeric<Double>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.doubleValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.doubleValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case FLOAT:
                query = new RangeQuery<Numeric<Float>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.floatValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.floatValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case BYTE:
                query = new RangeQuery<Numeric<Byte>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.longValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.byteValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case SHORT:
                query = new RangeQuery<Numeric<Short>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.shortValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.shortValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case INTEGER:
                query = new RangeQuery<Numeric<Integer>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.intValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.intValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case LONG:
                query = new RangeQuery<Numeric<Long>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, min.longValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, limit.longValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            default:
                throw new InvalidArgumentException("Cannot create a numeric range query for vector of "+vector.getBaseType());
        }
    }

    @SuppressWarnings({"unchecked", "ReuseOfLocalVariable", "OverlyLongMethod"})
    public IBitMap greaterThanOrEqualsQuery(Number limit, IVector vector, QueryState queryState, IBitMap filter) throws InvalidArgumentException, DataSpaceException {
        if (! vector.isNumeric()) {
            throw new InvalidArgumentException("Cannot create a numeric range query for vector of "+vector.getBaseType());
        }
        Number max = (Number) vector.getStats().getDescriptiveStats().getMax();
        RangeQuery query = null;
        IQueryParameterList params = null;
        switch (vector.getBaseType()) {
            case DOUBLE:
                query = new RangeQuery<Numeric<Double>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.doubleValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.doubleValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case FLOAT:
                query = new RangeQuery<Numeric<Float>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.floatValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.floatValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case BYTE:
                query = new RangeQuery<Numeric<Byte>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.byteValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.byteValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case SHORT:
                query = new RangeQuery<Numeric<Short>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.shortValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.shortValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case INTEGER:
                query = new RangeQuery<Numeric<Integer>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.intValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.intValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            case LONG:
                query = new RangeQuery<Numeric<Long>>();
                params = query.createParameterList(vector.getBaseType())
                        .set(RangeQuery.FROM, limit.longValue())
                        .set(RangeQuery.LEFT_CLOSED, true)
                        .set(RangeQuery.TO, max.longValue())
                        .set(RangeQuery.RIGHT_CLOSED, true);
                return query.apply(params, vector, queryState, filter); // unchecked
            default:
                throw new InvalidArgumentException("Cannot create a numeric range query for vector of "+vector.getBaseType());
        }
    }

    public Map<String, IScalar> getSingleRow(IntegerVector idVector, int id, DataFrame table, IQueryState queryState, String... requestedColumns) throws DataSpaceException, InvalidArgumentException {
        EqualsQuery<Numeric<Integer>> query = new EqualsQuery<Numeric<Integer>>();
        IQueryParameterList params = query.createParameterList(idVector.getBaseType()).set(EqualsQuery.VALUE, id);
        IBitMap bitMap = query.apply(params, idVector, queryState);
        IVectorIterator<Map<String, IScalar>> iterator = table.iterator(bitMap, requestedColumns);
        if (iterator.hasNext()) {
            return iterator.next(); // return the first match
        }
        else {
            return null;
        }
    }

    protected interface QueryApplyOperation {
        public<T extends IScalar> IBitMap apply(String methodName, IQueryParameterList params, IVector<T> vector, IQueryState queryState) throws DataSpaceException;
    }

    public static abstract class QueryDebugger {
        private long threshold;
        private long start=-1;

        public QueryDebugger(long minTimeToActivate) {
            this.threshold = minTimeToActivate;
        }

        public long getThreshold() {
            return threshold;
        }

        public DataSpace getDataSpace(IVector vector) {
            return vector.getDataSpace();
        }

        public IMemoryManager getMemoryManager(IVector vector) {
            return vector.getDataSpace().getMemoryManager();
        }

        @SuppressWarnings({"unchecked"})
        protected IBitMap perform(QueryApplyOperation op, String methodName, IQueryParameterList params, IVector vector, IQueryState queryState) throws DataSpaceException {
            prep(vector);
            start = System.currentTimeMillis();
            IBitMap result = null;
            try {
                result = op.apply(methodName, params, vector, queryState);
            }
            finally {
                if (System.currentTimeMillis()-start > threshold) {
                    debugQuery(methodName, params, vector, queryState, result);
                }
            }
            return result;
        }

        protected void prep(IVector vector) {
            // nothing by default
        }

        protected void debugQuery(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState, IBitMap result, long start) {
            if (System.currentTimeMillis()-start > threshold) {
                debugQuery(methodName, params, vector, queryState, result);
            }
        }

        public abstract void debugQuery(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState, IBitMap result);
    }

    public static class NullQueryDebugger extends QueryDebugger {

        public NullQueryDebugger() {
            super(1000);
        }

        @Override
        public void debugQuery(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState, IBitMap result) {
            // do nothing
        }
    }

    public static class QueryStateDump extends QueryDebugger {
        int swapInCounter = 0;
        long swapInTime = 0L;
        SimpleMemoryManager memoryManager = null;

        public QueryStateDump(long minTimeToActivate) {
            super(minTimeToActivate);
        }

        @Override
        protected void prep(IVector vector) {
            memoryManager = (SimpleMemoryManager) vector.getDataSpace().getMemoryManager();
            swapInCounter = memoryManager.getTotalSwapInCounter();
            swapInTime = memoryManager.getTotalSwapInTime();
        }

        @SuppressWarnings({"UseOfSystemOutOrSystemErr"})
        @Override
        public void debugQuery(String methodName, IQueryParameterList params, IVector vector, IQueryState queryState, IBitMap result) {
            System.out.println("Query state dump ===================================================");
            System.out.println("  Method: "+methodName);
            if (params!=null) {
                System.out.println("  Parameters: "+params);
            }
            System.out.println("  Result cardinality: "+result.cardinality());
            System.out.println("  Vector: "+vector.getClass().getSimpleName()+ "("+vector.getDescription()+")");
            System.out.println("  Vector size: "+vector.size());
            System.out.println("  Swap in count: "+(memoryManager.getTotalSwapInCounter()-swapInCounter));
            System.out.println("  Swap in time: "+(memoryManager.getTotalSwapInTime()-swapInTime));

            System.out.println("Query state:");
            ((QueryState)queryState).dump(false);
            System.out.println("Segment files:");
            AbstractVector.SegmentIterator iterator = ((AbstractVector) vector).segmentIterator();
            while (iterator.hasNext()) {
                AbstractVectorSegment segment = (AbstractVectorSegment) iterator.next().getSegment();
                String location = segment.getBackingArrayStorageLocation();
                System.out.println("  " + location+" "+(new File(location).length()/1024)+"kB");
            }
        }
    }
}
