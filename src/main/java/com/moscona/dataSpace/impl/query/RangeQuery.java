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

package com.moscona.dataSpace.impl.query;

import com.moscona.util.StringHelper;
import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.query.support.*;
import com.moscona.dataSpace.impl.segment.*;

/**
 * Created: 12/16/10 11:58 AM
 * By: Arnon Moscona
 * A query term that test for belonging to a (possibly open on either end) range of values (floats and doubles support resolution)
 */
@SuppressWarnings({"UnusedParameters", "OverlyCoupledClass"})
public class RangeQuery<T extends IScalar> extends AbstractQueryTerm<T> {
    public static final String FROM = "from";
    public static final String TO = "to";
    public static final String LEFT_CLOSED = "leftClosed";
    public static final String RIGHT_CLOSED = "rightClosed";
    private boolean leftClosed = true;
    private boolean rightClosed = true;

    public RangeQuery() {
        // do nothing?
    }

    /**
     * Called during query parameter list construction to have the subclass populate it with its parameters
     *
     * @param params
     * @param baseType
     */
    @Override
    protected void populateEmptyParameterList(QueryParameterList params, IVector.BaseType baseType) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                makeLongParameters(params);
                break;
            case DOUBLE:
            case FLOAT:
                makeDoubleParameters(params);
                break;
            case STRING:
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void makeDoubleParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new DoubleParameter(null, FROM, "low end of the range")).
                add(new DoubleParameter(null, TO, "high end of the range")).
                add(new BooleanParameter(true, LEFT_CLOSED, "whether to include the from value")).
                add(new BooleanParameter(false, RIGHT_CLOSED, "whether to include the to value"));   // by default floating queries are right-open
    }

    private void makeLongParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new LongParameter(null, FROM, "low end of the range")).
                add(new LongParameter(null, TO, "high end of the range")).
                add(new BooleanParameter(true, LEFT_CLOSED, "whether to include the from value")).
                add(new BooleanParameter(true, RIGHT_CLOSED, "whether to include the to value"));
    }

//    private void makeStringParameters(QueryParameterList params) throws DataSpaceException {
//        params.
//                add(new StringParameter(null, FROM, "low end of the range")).
//                add(new StringParameter(null, TO, "high end of the range")).
//                add(new BooleanParameter(true, LEFT_CLOSED, "whether to include the from value")).
//                add(new BooleanParameter(true, RIGHT_CLOSED, "whether to include the to value"));
//    }

    /**
     * Sets the parameters for this term before starting any evaluation. Do not perform any evaluation except parsing
     * the parameters, storing them (you won't get them again), and validating the. If you throw an exception here
     * then the query evaluation will be aborted.
     *
     * @param params
     * @param vector
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    protected void setParameters(IQueryParameterList params, IVector vector) throws DataSpaceException {
        validateNoSetParams(params);
        validateParamNames(params, vector.getBaseType());
        this.params = params;
        this.baseType = vector.getBaseType();
        leftClosed = ((BooleanParameter)params.get(LEFT_CLOSED)).getValue();
        rightClosed = ((BooleanParameter)params.get(RIGHT_CLOSED)).getValue();
    }

    /**
     * The subclass is required to tell us whether it can directly access the backing array for "bulk" evaluation or
     * that we need to iterate on its behalf. Bulk access is far faster than iteration and its preferable. If you choose
     * bulk access, you must ensure that you never modify the segment data.
     * If you choose bulk you do not have to implement the match(element) method.
     * If you do not choose bulk you do not need to implement the bulkMatch method
     *
     * @return
     */
    @Override
    protected boolean canProcessInBulk() {
        return true;
    }

    /**
     * An opportunity for the term to be evaluated based on the segment stats alone without looking at any of the
     * concrete data
     *
     * @param stats
     * @param segmentNumber
     * @param queryState
     * @return true or false if there is a uniform result to the whole segment, null if unable to determine
     */
    @Override
    protected Boolean quickMatch(ISegmentStats stats, int segmentNumber, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                return longQuickMatch(stats, segmentNumber, queryState);
            case DOUBLE:
            case FLOAT:
                return doubleQuickMatch(stats, segmentNumber, queryState, useResolution, resolution);
            case STRING:
            default:
                return null;
        }
    }

    private Boolean doubleQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState, boolean useResolution, double resolution) throws DataSpaceException {
        double from = doubleFrom();
        double to = doubleTo();
        double min = ((DoubleSegmentStats)stats).getMin();
        double max = ((DoubleSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }
        if (from > to) {
            throw new DataSpaceException("Invalid parameters from>to: "+from+">"+to);
        }

        return quickEval(from,to,min,max,leftClosed,rightClosed,useResolution,resolution);
    }

    private Double doubleTo() throws DataSpaceException {
        return ((DoubleParameter)params.get(TO)).getValue();
    }

    private Double doubleFrom() throws DataSpaceException {
        return ((DoubleParameter)params.get(FROM)).getValue();
    }

    private Boolean longQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) throws DataSpaceException {
        long from = longFrom();
        long to = longTo();
        long min = ((LongSegmentStats)stats).getMin();
        long max = ((LongSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }
        if (from > to) {
            throw new DataSpaceException("Invalid parameters from>to: "+from+">"+to);
        }

        return quickEval(from,to,min,max,leftClosed,rightClosed);

    }

    private Long longTo() throws DataSpaceException {
        return ((LongParameter)params.get(TO)).getValue();
    }

    private Long longFrom() throws DataSpaceException {
        return ((LongParameter)params.get(FROM)).getValue();
    }

    /**
     * Evaluates one data element at a time, returning true if it passed the match and false otherwise. This is much
     * less efficient than bulk matching, but is easier to implement and safer (immutability is guaranteed)
     *
     * @param element
     * @param queryState
     * @return
     */
    @Override
    protected boolean match(IScalar element, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                long lvalue = element.getLongValue();
                return (leftClosed ? lvalue >= longFrom() : lvalue > longFrom()) &&
                        (rightClosed ? lvalue <= longTo() : lvalue < longTo());

            case DOUBLE:
            case FLOAT:
                double dvalue = element.getDoubleValue();
                Double from = doubleFrom();
                Double to = doubleTo();
                return (leftClosed ? (dvalue >= from || (useResolution && equals(dvalue,from,resolution))) : dvalue > from) &&
                        (rightClosed ? (dvalue <= to || (useResolution && equals(dvalue,to,resolution))) : dvalue < to);
            case STRING:
            default:
                return false;
        }
    }

    /**
     * Evaluates the segment as a whole using direct access to its backing array
     * This is where the real efficiency (and ugliness kicks in - direct access to backing arrays as primitive types
     * this is geared for high speed queries on very large segments (1 million long is considered good).
     * 6 copies of the same method differing only in the primitive type...
     * @param segmentInfo
     * @param progressiveResult
     * @param queryState
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    protected void bulkMatch(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException {
        switch (baseType) {
            case LONG:
                bulkMatchLong(segmentInfo, progressiveResult);
                return;
            case INTEGER:
                bulkMatchInteger(segmentInfo, progressiveResult);
                return;
            case SHORT:
                bulkMatchShort(segmentInfo, progressiveResult);
                return;
            case BYTE:
                bulkMatchByte(segmentInfo, progressiveResult);
                return;
            case DOUBLE:
                bulkMatchDouble(segmentInfo, progressiveResult, useResolution, resolution);
                return;
            case FLOAT:
                bulkMatchFloat(segmentInfo, progressiveResult, useResolution, resolution);
                return;
            case STRING:
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void bulkMatchFloat(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution) throws DataSpaceException {
        try {
            double from = doubleFrom();
            double to = doubleTo();

            float[] values = ((FloatSegmentBackingArray) ((FloatSegment) segmentInfo.getSegment()).getBackingArray()).data;
            // HOLD  benchmark what the improvement would be if we move the leftClosed and rightClosed tests outside the loop (ending up with 8 different possible loops, but possibly improving the performance by further 10%
            if (useResolution) {
                for (double value : values) {
                    progressiveResult.add(
                        (leftClosed  ? (value > from || Math.abs(value-from)<=resolution) : value > from) &&
                        (rightClosed ? (value < to   || Math.abs(value-to)<=resolution)   : value < to));
                }
            }
            else {
                for (double value : values) {
                    progressiveResult.add(
                        (leftClosed  ? (value >= from) : (value > from)) &&
                        (rightClosed ? (value <= to)   : value < to));
                }
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (float): "+e,e);
        }
    }

    private void bulkMatchDouble(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution) throws DataSpaceException {
        try {
            double from = doubleFrom();
            double to = doubleTo();

            double [] values = ((DoubleSegmentBackingArray) ((DoubleSegment) segmentInfo.getSegment()).getBackingArray()).data;

//          // in the following inlining the Math.abs() instead of calling equals(...) gave us a 2% performance improvment. Worth keeping for this heavy use query. Inlining the Math.abs to (value>from ? value-from<=resolution : from-value<=resolution) actually *degrades* performance by a full 10% over the selected implementation...
            // a further improvement was moving the conditional on useResolution outside of the loop, gaining 10% in performance over the last checked in version with a benchmark of 12.5ms
            // HOLD  benchmark what the improvement would be if we move the leftClosed and rightClosed tests outside the loop (ending up with 8 different possible loops, but possibly improving the performance by further 10%

            if (useResolution) {
                for (double value : values) {
                    progressiveResult.add(
                        (leftClosed  ? (value > from || Math.abs(value-from)<=resolution) : value > from) &&
                        (rightClosed ? (value < to|| Math.abs(value-to)<=resolution)   : value < to));
                }
            }
            else {
                for (double value : values) {
                    progressiveResult.add(
                        (leftClosed  ? (value >= from) : (value > from)) &&
                        (rightClosed ? (value <= to)   : value < to));
                }
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (double): "+e,e);
        }
    }

    private void bulkMatchByte(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            long from = longFrom();
            long to = longTo();

            byte[] values = ((ByteSegmentBackingArray) ((ByteSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (byte value : values) {
                progressiveResult.add(
                        (leftClosed  ? value >= from : value > from) &&
                        (rightClosed ? value <= to   : value < to));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (byte): "+e,e);
        }
    }

    private void bulkMatchShort(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            long from = longFrom();
            long to = longTo();

            short[] values = ((ShortSegmentBackingArray) ((ShortSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (short value : values) {
                progressiveResult.add(
                        (leftClosed  ? value >= from : value > from) &&
                        (rightClosed ? value <= to   : value < to));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (short): "+e,e);
        }
    }

    private void bulkMatchInteger(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
       try {
            long from = longFrom();
            long to = longTo();

            int[] values = ((IntegerSegmentBackingArray) ((IntegerSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (int value : values) {
                progressiveResult.add(
                        (leftClosed  ? value >= from : value > from) &&
                        (rightClosed ? value <= to   : value < to));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (int): "+e,e); 
        }
    }

    private void bulkMatchLong(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            long from = longFrom();
            long to = longTo();

            long[] values = ((LongSegmentBackingArray) ((LongSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (long value : values) {
                progressiveResult.add(
                        (leftClosed  ? value >= from : value > from) &&
                        (rightClosed ? value <= to   : value < to));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (long): "+e,e);
        }
    }


    @Override
    public String toString(IQueryParameterList params) {
        IQueryParameterList original = this.params;
        this.params = params;
        String retval = leftClosed ? "in [" : "(";
        String from = "?";
        String to = "?";

        try {
            switch (baseType) {
                case LONG:
                case INTEGER:
                case SHORT:
                case BYTE:
                    from = StringHelper.prettyPrint(longFrom());
                    to = StringHelper.prettyPrint(longTo());
                    break;
                case DOUBLE:
                case FLOAT:
                    from = StringHelper.prettyPrint(doubleFrom());
                    to = StringHelper.prettyPrint(doubleTo());
                    break;
                case STRING:
                default:
                    from = to = "Unsupported type for range query";
            }

        }
        catch (DataSpaceException e) {
            return "{Exception int RangeQuery.toString(): "+e+"}";
        }

        retval += from+".."+to;
        retval += (rightClosed ? "]" : ")");
        this.params = original;

        return retval;
    }
}
