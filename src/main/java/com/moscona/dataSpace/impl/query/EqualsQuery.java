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
 * Created: 12/22/10 8:23 AM
 * By: Arnon Moscona
 * A query term that test for equality to a single value (floats and doubles support resolution)
 */
@SuppressWarnings({"UnusedParameters"})
public class EqualsQuery<T extends IScalar> extends AbstractQueryTerm<T> {
    public static final String VALUE = "value";

    // only one of the following will be used in actuality
    private double doubleValue;
    private long longValue;
    private String stringValue;
    private int intValue;
    private boolean booleanValue;

    public EqualsQuery() {
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
                makeStringParameters(params);
                break;
            case BOOLEAN:
                makeBooleanParameters(params);
                break;
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void makeDoubleParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new DoubleParameter(null, VALUE, "the value to match"));
    }

    private void makeLongParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new LongParameter(null, VALUE, "the value to match"));
    }

    private void makeStringParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new StringParameter(null, VALUE, "the value to match"));
    }

    private void makeBooleanParameters(QueryParameterList params) throws DataSpaceException {
        params.
                add(new BooleanParameter(null, VALUE, "the value to match"));
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
        IQueryParameter param = params.get(VALUE);
        IQueryParameter.ParameterType paramBaseType = param.getType();

        switch (paramBaseType) {
            case BOOLEAN:
                if (baseType != IVector.BaseType.BOOLEAN) {
                    throwIncompatibleParameterException(VALUE, baseType, paramBaseType);
                }
                booleanValue = (Boolean)param.getValue();
                break;
            case LONG:
                applyValueParam((Long)param.getValue(), paramBaseType, vector.getDataSpace());
                break;
            case DOUBLE:
                applyValueParam((Double)param.getValue(), paramBaseType);
                break;
            case STRING:
                applyValueParam((String) param.getValue(), paramBaseType, vector.getDataSpace());
                break;
        }
    }

    private void applyValueParam(String value, IQueryParameter.ParameterType paramBaseType, DataSpace ds) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                longValue = Long.parseLong(value.replaceAll(",", ""));
                break;
            case DOUBLE:
            case FLOAT:
                doubleValue = Double.parseDouble(value.replaceAll(",",""));
                break;
            case STRING:
                stringValue = value;
                intValue = ds.getCode(value);
                break;
            case BOOLEAN:
                booleanValue = toBoolean(value);
                break;
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void applyValueParam(Double value, IQueryParameter.ParameterType paramBaseType) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                longValue = Math.round(value);
                break;
            case DOUBLE:
            case FLOAT:
                doubleValue = value;
                break;
            case STRING:
                throwIncompatibleParameterException(VALUE,baseType, paramBaseType); // there is no reasonable conversion
                break;
            case BOOLEAN:
                throwIncompatibleParameterException(VALUE,baseType, paramBaseType);
                break;
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void applyValueParam(long value, IQueryParameter.ParameterType paramBaseType, DataSpace ds) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                longValue = value;
                break;
            case DOUBLE:
            case FLOAT:
                doubleValue = value;
                break;
            case STRING:
                stringValue = Long.toString(value);
                intValue = ds.getCode(stringValue);
                break;
            case BOOLEAN:
                throwIncompatibleParameterException(VALUE,baseType, paramBaseType);
                break;
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
                return stringQuickMatch(stats, segmentNumber, queryState);
            case BOOLEAN:
                return booleanQuickMatch(stats, segmentNumber, queryState);
            default:
                return null;
        }
    }

    private Boolean booleanQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) {
        boolean min = ((BooleanSegmentStats)stats).getMin();
        boolean max = ((BooleanSegmentStats)stats).getMax();
        return quickEval(booleanValue, min, max);
    }

    private Boolean stringQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) {
        String min = ((StringSegmentStats)stats).getMin();
        String max = ((StringSegmentStats)stats).getMax();
        return quickEval(stringValue, min, max, true, true);
    }

    private Boolean doubleQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState, boolean useResolution, double resolution) throws DataSpaceException {
        double min = ((DoubleSegmentStats)stats).getMin();
        double max = ((DoubleSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }

        return quickEval(doubleValue, doubleValue, min, max, true, true, useResolution, resolution);
    }

    private Boolean longQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) throws DataSpaceException {
        long min = ((LongSegmentStats)stats).getMin();
        long max = ((LongSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }

        return quickEval(longValue, longValue, min, max, true, true);
    }

    /**
     * Evaluates one data element at a time, returning true if it passed the match and false otherwise. This is much
     * less efficient than bulk matching, but is easier to implement and safer (immutability is guaranteed)
     *
     * @param element
     * @param queryState
     * @return
     */
    @SuppressWarnings({"FloatingPointEquality"}) // by default we'll be using resolution based comparison
    @Override
    protected boolean match(IScalar element, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                return longValue == element.getLongValue();
            case DOUBLE:
            case FLOAT:
                return equals(element.getDoubleValue(),doubleValue,resolution,useResolution);
            case STRING:
                return stringValue.equals(element.toString());
            case BOOLEAN:
                try {
                    return booleanValue == ((Logical)element).getValue();
                }
                catch (Exception e) {
                    throw new DataSpaceException("Failed to match element: "+element+" to boolean "+booleanValue);
                }
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
                bulkMatchString(segmentInfo, progressiveResult);
                return;
            case BOOLEAN:
                bulkMatchBoolean(segmentInfo, progressiveResult);
                return;
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void bulkMatchBoolean(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            boolean[] values = ((BooleanSegmentBackingArray) ((LogicalSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (boolean value : values) {
                progressiveResult.add(value==booleanValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (boolean): "+e,e);
        }
    }

    private void bulkMatchString(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            int[] values = ((StringSegmentBackingArray) ((StringSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (int value : values) {
                progressiveResult.add(value == intValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (string): "+e,e);
        }
    }

    private void bulkMatchFloat(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution) throws DataSpaceException {
        try {
            float[] values = ((FloatSegmentBackingArray) ((FloatSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (float value : values) {
                progressiveResult.add(equals(doubleValue,value,resolution,useResolution));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (float): "+e,e);
        }
    }

    private void bulkMatchDouble(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution) throws DataSpaceException {
        try {
            double [] values = ((DoubleSegmentBackingArray) ((DoubleSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (double value : values) {
                progressiveResult.add(equals(doubleValue,value,resolution,useResolution));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (double): "+e,e);
        }
    }

    private void bulkMatchByte(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            byte[] values = ((ByteSegmentBackingArray) ((ByteSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (byte value : values) {
                progressiveResult.add(value==longValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (byte): "+e,e);
        }
    }

    private void bulkMatchShort(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            short[] values = ((ShortSegmentBackingArray) ((ShortSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (short value : values) {
                progressiveResult.add(value==longValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (short): "+e,e);
        }
    }

    private void bulkMatchInteger(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
       try {
            int[] values = ((IntegerSegmentBackingArray) ((IntegerSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (int value : values) {
                progressiveResult.add(value==longValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (int): "+e,e);
        }
    }

    private void bulkMatchLong(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            long[] values = ((LongSegmentBackingArray) ((LongSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (long value : values) {
                progressiveResult.add(value==longValue);
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (long): "+e,e);
        }
    }


    @Override
    public String toString(IQueryParameterList params) {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                return " = "+longValue;
            case DOUBLE:
            case FLOAT:
                return " = " + StringHelper.prettyPrint(doubleValue);
            case STRING:
                return " = '"+stringValue+"'";
            case BOOLEAN:
                return " = "+booleanValue;
            default:
                return "Unsupported type for equals query";
        }
    }

}
