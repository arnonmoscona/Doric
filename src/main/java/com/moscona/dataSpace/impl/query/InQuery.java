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

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.query.support.AbstractQueryTerm;
import com.moscona.dataSpace.impl.query.support.LongSetParameter;
import com.moscona.dataSpace.impl.query.support.QueryParameterList;
import com.moscona.dataSpace.impl.query.support.StringSetParameter;
import com.moscona.dataSpace.impl.segment.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Created: 12/22/10 3:52 PM
 * By: Arnon Moscona
 * A query term that test for belonging to a specified set of values (discrete types only: integral numerics and strings)
 */
public class InQuery<T extends IScalar> extends AbstractQueryTerm<T> {
    public static final String VALUES = "values";


    /*
    Performance comment:
    I benchmarked three different implementation of set on String arrays:
    - java.util.HashSet
    - net.ontopia.utils.CompactHashSet
    - com.google.common.collect.ImmutableSet
    The test was a repeated serch over a vector of a million symbols - repeating out of 500 distinct symbols.
    I used actual symbols rom a day buffer to make the simulation realistic for stock analysis.
    The java.util.HashSet won hands down both for a set of 5 and for a set of 50 (tested on Java 6 64bit JDK 1.6.0_18)
    On average the Google immutable set was 26% slower than the JDK implementation and the ontopia implementation
    was 91% slower.
    So the conclusion is that the google implementation has a semantic advantage but a significant performance penalty.
    The CompactHashSet is totally out of the question. There is a high chance that the results are seriously skewed by
    the data distribution, but I only care about my use case here.
    In conclusion - I'm keeping the semantically lame defensive copy to maintain the performance advantage.
     */

    // only one of the following will be used in actuality
    private Set<Integer> stringSetValue=null;  // IMPORTANT strings are coded as integers and are converted only when needed
    private Set<Long> longSetValue=null;
    private DataSpace dataSpace = null;

    public InQuery() {
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
                throwIncompatibleException(baseType);
                break;
            case STRING:
                makeStringParameters(params);
                break;
            case BOOLEAN:
                throwIncompatibleException(baseType);
                break;
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void makeLongParameters(QueryParameterList params) throws DataSpaceException {
        params.add(new LongSetParameter(null, VALUES, "the list of values to match against"));
    }

    private void makeStringParameters(QueryParameterList params) throws DataSpaceException {
        params.add(new StringSetParameter(null, VALUES, "the list of values to match against"));
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
    @SuppressWarnings({"unchecked"})
    @Override
    protected void setParameters(IQueryParameterList params, IVector vector) throws DataSpaceException {
        validateParamNames(params, vector.getBaseType());
        this.params = params;
        this.baseType = vector.getBaseType();
        IQueryParameter param = params.get(VALUES);
        IQueryParameter.ParameterType paramBaseType = param.getType();
        validateVectorCompatibility(paramBaseType, vector);
        dataSpace = vector.getDataSpace();

        switch (paramBaseType) {
            case STRING_SET:
                stringSetValue = makeStringSet(param.getValueSet());
                break;
            case LONG_SET:
                longSetValue = makeLongSet(param.getValueSet());
                break;
            case BOOLEAN:
            case LONG:
            case DOUBLE:
            case STRING:
                throw new DataSpaceException("Incompatible parameter type: "+paramBaseType+ " this only takes sets, no scalars");
        }
    }

    private Set<Integer> makeStringSet(Set valueSet) {
        HashSet<Integer> retval = new HashSet<Integer>();
        for (Object o: valueSet) {
            retval.add(dataSpace.getCode(o.toString()));
        }
        return retval;
    }

    private Set<Long> makeLongSet(Set valueSet) {
        HashSet<Long> retval = new HashSet<Long>();
        for (Object o: valueSet) {
            if (Number.class.isAssignableFrom(o.getClass())) {
                retval.add(((Number)o).longValue());
            }
            else {
                retval.add(Long.parseLong(o.toString()));
            }
        }
        return retval;
    }

    private void validateVectorCompatibility(IQueryParameter.ParameterType paramBaseType, IVector vector) throws DataSpaceException {
        switch (baseType) {
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                switch (paramBaseType) {
                    case STRING_SET:
                        convertStringsToLongs();
                        break;
                    case LONG_SET:
                        break;
                }
                break;
            case STRING:
                switch (paramBaseType) {
                    case STRING_SET:
                        break;
                    case LONG_SET:
                        convertLongsToStrings();
                        break;
                }
                break;
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            default:
                throwIncompatibleException(baseType);
        }
    }

    private void convertLongsToStrings() {
        stringSetValue = new HashSet<Integer>();
        for (long value: longSetValue) {
            stringSetValue.add(dataSpace.getCode(Long.toString(value)));
        }
    }

    private void convertStringsToLongs() throws DataSpaceException {
        Integer currentString = null;
        try {
            longSetValue = new HashSet<Long>();
            for (Integer str: stringSetValue) {
                currentString = str;
                longSetValue.add(Long.parseLong(dataSpace.decodeToString(str).trim().replaceAll(",", "")));
            }
        }
        catch (NumberFormatException e) {
            throw new DataSpaceException("Number format exception. Could not parse "+currentString);
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
            case STRING:
                return stringQuickMatch(stats, segmentNumber, queryState);
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            default:
                return null;
        }
    }

    private Boolean stringQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) {
        String min = ((StringSegmentStats)stats).getMin();
        String max = ((StringSegmentStats)stats).getMax();

        int trues = 0; // counts for how many values we accepted everything
        int falses = 0; // counts for how many values we rejected everything

        for (Integer candidate: stringSetValue) {
            Boolean result = quickEval(dataSpace.decodeToString(candidate), min, max, true, true);
            if (result==null) {
                return null; // not sure about one => not sure about all
            }
            if(result) {
                trues++;
            }
            else {
                falses++;
            }
        }
        if (trues>0 && falses==0) {
            return true;
        }
        if (trues==0 && falses>0) {
            return false;
        }
        return null;
    }

    private Boolean longQuickMatch(ISegmentStats stats, int segmentNumber, IQueryState queryState) throws DataSpaceException {
        long min = ((LongSegmentStats)stats).getMin();
        long max = ((LongSegmentStats)stats).getMax();

        if (min > max) {
            throw new DataSpaceException("Invalid segment stats min>max: "+min+">"+max);
        }

        int trues = 0; // counts for how many values we accepted everything
        int falses = 0; // counts for how many values we rejected everything

        for (long candidate: longSetValue) {
            Boolean result = quickEval(candidate, candidate, min, max, true, true);
            if (result==null) {
                return null; // not sure about one => not sure about all
            }
            if(result) {
                trues++;
            }
            else {
                falses++;
            }
        }
        if (trues>0 && falses==0) {
            return true;
        }
        if (trues==0 && falses>0) {
            return false;
        }
        return null;
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
                return longSetValue.contains(element.getLongValue());
            case STRING:
                return stringSetValue.contains(dataSpace.getCode(element.toString()));
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
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
            case STRING:
                bulkMatchString(segmentInfo, progressiveResult);
                return;
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            default:
                applyToAll(false,segmentInfo,progressiveResult,queryState);
        }
    }

    private void bulkMatchString(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            int[] values = ((StringSegmentBackingArray) ((StringSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (int value : values) {
                progressiveResult.add(stringSetValue.contains(value));
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while bulk matching segment (string): "+e,e);
        }
    }

    private void bulkMatchByte(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult) throws DataSpaceException {
        try {
            byte[] values = ((ByteSegmentBackingArray) ((ByteSegment) segmentInfo.getSegment()).getBackingArray()).data;

            for (byte value : values) {
                progressiveResult.add(longSetValue.contains((long)value));
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
                progressiveResult.add(longSetValue.contains((long)value));
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
                progressiveResult.add(longSetValue.contains((long)value));
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
                progressiveResult.add(longSetValue.contains(value));
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
                return "in ["+ StringUtils.join(longSetValue,", ")+"]";
            case STRING:
                ArrayList<String> decoded = new ArrayList<String>();
                for (int code: stringSetValue) {
                    decoded.add(dataSpace.decodeToString(code));
                }
                return "in ['"+ StringUtils.join(decoded,"', '")+"']";
            case DOUBLE:
            case FLOAT:
            case BOOLEAN:
            default:
                return "Unsupported type for in (...) query";
        }
    }


}
