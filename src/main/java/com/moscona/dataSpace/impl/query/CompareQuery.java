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

import com.moscona.exceptions.StackTraceHelper;
import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.query.support.*;
import com.moscona.dataSpace.impl.segment.*;

/**
 * Created: 12/29/10 10:46 AM
 * By: Arnon Moscona
 */
public class CompareQuery<T extends IScalar> extends AbstractQueryTerm<T> {
    private enum Operator {GT,GE,LT,LE};
    public static final String COMPARE_TO = "compare to";
    public static final String OPERATOR = "operator";

    private double doubleBoundary = 0.0;
    private long longBoundary = 0L;
    private Operator operator = null;

    public CompareQuery() {
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
                add(new DoubleParameter(null, COMPARE_TO, "value to compare to")).
                add(new StringParameter(null, OPERATOR, "comparison operator (<,<=,>,>=)"));
    }

    private void makeLongParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new LongParameter(null, COMPARE_TO, "value to compare to")).
                add(new StringParameter(null, OPERATOR, "comparison operator (<,<=,>,>=)"));
    }

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
        String op = params.get(OPERATOR).getValue().toString().trim();
        if (op.equals("<")) {
            operator = Operator.LT;
        }
        else if (op.equals("<=")) {
            operator = Operator.LE;
        }
        else if (op.equals(">")) {
            operator = Operator.GT;
        }
        else if (op.equals(">=")) {
            operator = Operator.GE;
        }
        else {
            throw new DataSpaceException("Cannot parse comparison operator \""+op+"\"");
        }

        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                longBoundary = ((LongParameter) params.get(COMPARE_TO)).getValue();
                break;
            case DOUBLE:
            case FLOAT:
                doubleBoundary = ((DoubleParameter) params.get(COMPARE_TO)).getValue();
                break;
            case STRING:
            default:
                throwIncompatibleException(baseType);
        }
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
        double min = ((DoubleSegmentStats)stats).getMin();
        double max = ((DoubleSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }

        switch(operator) {
            case LT:
                return (max < doubleBoundary && !(useResolution && equals(max,doubleBoundary,resolution))) ? true : null;
            case LE:
                return (max < doubleBoundary || (useResolution && equals(max,doubleBoundary,resolution))) ? true : null;
            case GT:
                return (min > doubleBoundary && !(useResolution && equals(min,doubleBoundary,resolution))) ? true : null;
            case GE:
                return (min > doubleBoundary || (useResolution && equals(min,doubleBoundary,resolution))) ? true : null;
        }

        throw new DataSpaceException("Should not have reached this line!");
    }

    private Boolean longQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) throws DataSpaceException {
        long min = ((LongSegmentStats)stats).getMin();
        long max = ((LongSegmentStats)stats).getMax();

        switch(operator) {
            case LT:
                return max < longBoundary ? true : null;
            case LE:
                return max <= longBoundary ? true : null;
            case GT:
                return min > longBoundary? true : null;
            case GE:
                return min >= longBoundary? true : null;
        }

        throw new DataSpaceException("Should not have reached this line!");
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
                switch(operator) {
                    case LT:
                        return lvalue < longBoundary;
                    case LE:
                        return lvalue <= longBoundary;
                    case GT:
                        return lvalue > longBoundary;
                    case GE:
                        return lvalue >= longBoundary;
                }

            case DOUBLE:
            case FLOAT:
                double dvalue = element.getDoubleValue();
                switch(operator) {
                    case LT:
                        return dvalue < doubleBoundary && !(useResolution && equals(dvalue,doubleBoundary,resolution));
                    case LE:
                        return dvalue <= doubleBoundary || (useResolution && equals(dvalue,doubleBoundary,resolution));
                    case GT:
                        return dvalue > doubleBoundary && !(useResolution && equals(dvalue,doubleBoundary,resolution));
                    case GE:
                        return dvalue >= doubleBoundary || (useResolution && equals(dvalue,doubleBoundary,resolution));
                }
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
            float[] values = ((FloatSegmentBackingArray) ((FloatSegment) segmentInfo.getSegment()).getBackingArray()).data;
            if (useResolution) {
                switch (operator) {
                    case LT:
                        for (float value : values) {
                            progressiveResult.add(value<doubleBoundary && ! (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case LE:
                        for (float value : values) {
                            progressiveResult.add(value<=doubleBoundary || (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case GT:
                        for (float value : values) {
                            progressiveResult.add(value>doubleBoundary && ! (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case GE:
                        for (float value : values) {
                            progressiveResult.add(value>=doubleBoundary || (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    default:
                        throw new DataSpaceException("Should not have gotten to this line!");
                }
            }
            else {
                // do not use resolution
                switch (operator) {
                    case LT:
                        for (float value : values) {
                            progressiveResult.add(value<doubleBoundary);
                        }
                        break;
                    case LE:
                        for (float value : values) {
                            progressiveResult.add(value<=doubleBoundary);
                        }
                        break;
                    case GT:
                        for (float value : values) {
                            progressiveResult.add(value>doubleBoundary);
                        }
                        break;
                    case GE:
                        for (float value : values) {
                            progressiveResult.add(value>=doubleBoundary);
                        }
                        break; 
                    default:
                        throw new DataSpaceException("Should not have gotten to this line!");
                }
            }
        }
        catch (DataSpaceException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (float): "+e,e);
        }
    }

    private void bulkMatchDouble(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution) throws DataSpaceException {
        try {
            double [] values = ((DoubleSegmentBackingArray) ((DoubleSegment) segmentInfo.getSegment()).getBackingArray()).data;

            if (useResolution) {
                switch (operator) {
                    case LT:
                        for (double value : values) {
                            progressiveResult.add(value<doubleBoundary && ! (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case LE:
                        for (double value : values) {
                            progressiveResult.add(value<=doubleBoundary || (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case GT:
                        for (double value : values) {
                            progressiveResult.add(value>doubleBoundary && ! (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    case GE:
                        for (double value : values) {
                            progressiveResult.add(value>=doubleBoundary || (Math.abs(value-doubleBoundary)<=resolution));
                        }
                        break;
                    default:
                        throw new DataSpaceException("Should not have gotten to this line!");
                }
            }
            else {
                // do not use resolution
                switch (operator) {
                    case LT:
                        for (double value : values) {
                            progressiveResult.add(value<doubleBoundary);
                        }
                        break;
                    case LE:
                        for (double value : values) {
                            progressiveResult.add(value<=doubleBoundary);
                        }
                        break;
                    case GT:
                        for (double value : values) {
                            progressiveResult.add(value>doubleBoundary);
                        }
                        break;
                    case GE:
                        for (double value : values) {
                            progressiveResult.add(value>=doubleBoundary);
                        }
                        break; 
                    default:
                        throw new DataSpaceException("Should not have gotten to this line!");
                }
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (double): "+e,e);
        }
    }

    private void bulkMatchByte(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            byte[] values = ((ByteSegmentBackingArray) ((ByteSegment) segmentInfo.getSegment()).getBackingArray()).data;

            switch (operator) {
                case LT:
                    for (byte value: values) {
                        progressiveResult.add(value<longBoundary);
                    }
                    break;
                case LE:
                    for (byte value: values) {
                        progressiveResult.add(value<=longBoundary);
                    }
                    break;
                case GT:
                    for (byte value: values) {
                        progressiveResult.add(value>longBoundary);
                    }
                    break;
                case GE:
                    for (byte value: values) {
                        progressiveResult.add(value>=longBoundary);
                    }
                    break;
                default:
                    throw new DataSpaceException("Should not have gotten to this line!");
            }
        }
        catch (DataSpaceException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (byte): "+e,e);
        }
    }

    private void bulkMatchShort(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            short[] values = ((ShortSegmentBackingArray) ((ShortSegment) segmentInfo.getSegment()).getBackingArray()).data;

            switch (operator) {
                case LT:
                    for (short value: values) {
                        progressiveResult.add(value<longBoundary);
                    }
                    break;
                case LE:
                    for (short value: values) {
                        progressiveResult.add(value<=longBoundary);
                    }
                    break;
                case GT:
                    for (short value: values) {
                        progressiveResult.add(value>longBoundary);
                    }
                    break;
                case GE:
                    for (short value: values) {
                        progressiveResult.add(value>=longBoundary);
                    }
                    break;
                default:
                    throw new DataSpaceException("Should not have gotten to this line!");
            }
        }
        catch (DataSpaceException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (byte): "+e,e);
        }
    }

    private void bulkMatchInteger(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
       try {
            int[] values = ((IntegerSegmentBackingArray) ((IntegerSegment) segmentInfo.getSegment()).getBackingArray()).data;

            switch (operator) {
                case LT:
                    for (int value: values) {
                        progressiveResult.add(value<longBoundary);
                    }
                    break;
                case LE:
                    for (int value: values) {
                        progressiveResult.add(value<=longBoundary);
                    }
                    break;
                case GT:
                    for (int value: values) {
                        progressiveResult.add(value>longBoundary);
                    }
                    break;
                case GE:
                    for (int value: values) {
                        progressiveResult.add(value>=longBoundary);
                    }
                    break;
                default:
                    throw new DataSpaceException("Should not have gotten to this line!");
            }
        }
        catch (DataSpaceException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (byte): "+e,e);
        }
    }

    private void bulkMatchLong(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            long[] values = ((LongSegmentBackingArray) ((LongSegment) segmentInfo.getSegment()).getBackingArray()).data;

            switch (operator) {
                case LT:
                    for (long value: values) {
                        progressiveResult.add(value<longBoundary);
                    }
                    break;
                case LE:
                    for (long value: values) {
                        progressiveResult.add(value<=longBoundary);
                    }
                    break;
                case GT:
                    for (long value: values) {
                        progressiveResult.add(value>longBoundary);
                    }
                    break;
                case GE:
                    for (long value: values) {
                        progressiveResult.add(value>=longBoundary);
                    }
                    break;
                default:
                    throw new DataSpaceException("Should not have gotten to this line!");
            }
        }
        catch (DataSpaceException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (long): "+e,e);
        }
    }


    @Override
    public String toString(IQueryParameterList params) {
        String retval = null;
        switch (operator) {
            case LT:
                retval = "< ";
                break;
            case LE:
                retval = "<= ";
                break;
            case GT:
                retval = "> ";
                break;
            case GE:
                retval = ">= ";
                break;
            default:
                return "Should not have gotten to this line! "+ StackTraceHelper.thisLineLocation();
        }

        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                return retval+longBoundary;
            case DOUBLE:
            case FLOAT:
                return retval+doubleBoundary;
            case STRING:
            default:
                return "Unsupported type for compare query: "+baseType;
        }

    }
}

