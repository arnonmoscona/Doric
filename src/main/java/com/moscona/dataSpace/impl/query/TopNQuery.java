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
import com.moscona.dataSpace.impl.query.support.AbstractHistogramBasedTransformer;
import com.moscona.dataSpace.impl.segment.*;

import java.util.*;

/**
 * Created: 12/30/10 4:00 PM
 * By: Arnon Moscona
 * Performs top/bottom N queries for small N values.
 * two parameters: isTop:boolean, N:short
 * Technically, this is not a query, but a transformer. To perform a real query (with a bitmap result) do one of the following:
 * - Use a TopNQuery to obtain the top N values
 * - Use a range query to query for the range of the minimum and maximum of the returned vector
 * - Or make a set out of the values and do an in query on the set (useful for Strings)
 *
 *
 * IMPORTANT this is not a very efficient method and should only be used on smallish vectors (e.g. subsets after performing some filtering query)
 */
public class TopNQuery<T extends IScalar>  extends AbstractHistogramBasedTransformer<T> {
    public static final int MAX_N = 10000;
    private boolean isTop;
    private short n;
    private PriorityQueue<String> topNString;
    private PriorityQueue<Long> topNLong;
    private boolean useResolution = false;
    private boolean isFirst = true;

    /**
     * Creates a topN query. It produces a histogram of the top/bottom N values in the vector with a count of how many
     * times each of them occurs. So technically, the result could represent more than N values in the vector, as it
     * represents N distinct values
     * @param isTop if true this is a Top N, otherwise it's a Bottom N
     * @param n the number of top/bottom values to track (with resolution for real numbers)
     */
    public TopNQuery(boolean isTop, short n) throws DataSpaceException {
        super();
        if (n> MAX_N) {
            throw new DataSpaceException("This TopN implementation is intended for small numbers. " +
                    "The provided N="+n+" exceeds the maximum allowed of "+MAX_N);
        }

        this.isTop = isTop;
        this.n = n;
        if (isTop) {
            topNLong = new PriorityQueue<Long>(n+1);
            topNString = new PriorityQueue<String>(n+1);
        }
        else {
            // Bottom N - reverse comparison
            topNLong = new PriorityQueue<Long>(n+1, new Comparator<Long>() {
                @Override
                public int compare(Long o1, Long o2) {
                    return o2.compareTo(o1);
                }
            });
            topNString = new PriorityQueue<String>(n+1, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o2.compareTo(o1);
                }
            });
        }
    }

    @Override
    protected void initializeTransformation(IVector<T> vector) throws DataSpaceException {
        dataSpace = vector.getDataSpace();
        baseType = vector.getBaseType();
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                resolution = ((INumericResolutionSupport)vector).getResolution();
                useResolution = true;
                break;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case STRING:
                useResolution = false;
                break;
            case BOOLEAN:
            default:
                throw new DataSpaceException("Don't know how to do TopN for "+baseType);
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    protected boolean quickTransform(ISegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                     boolean useResolution, double resolution,
                                     IQueryState queryState) throws DataSpaceException {
        // does not implement filtering as the quick transform works correctly without the filtering
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                return realQuickTransform((DoubleSegmentStats)stats);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return longQuickTransform((LongSegmentStats)stats);
            case STRING:
                return stringQuickTransform((StringSegmentStats)stats);
            case BOOLEAN:
            default:
                throw new DataSpaceException("Don't know how to do TopN for "+baseType);
        }
    }

    private boolean realQuickTransform(DoubleSegmentStats stats) {
        if (topNLong.isEmpty()) {
            return false;
        }

        long bottom = topNLong.peek();
        long test = isTop ? round(stats.getMax()) : round(stats.getMin());
        return isTop ? test<bottom : test>bottom;
    }

    private boolean longQuickTransform(LongSegmentStats stats) throws DataSpaceException {
        // if the segment is all smaller (in case of top) than the minimum of the priority queue then we can eliminate
        // the whole segment
        if (topNLong.isEmpty()) {
            return false;
        }

        long bottom = topNLong.peek();
        long test = isTop ? stats.getMax() : stats.getMin();
        return isTop ? test<bottom : test>bottom;
    }

    private boolean stringQuickTransform(StringSegmentStats stats) {
        if (topNString.isEmpty()) {
            return false;
        }

        // if the segment is all smaller (in case of top) than the minimum of the priority queue then we can eliminate
        // the whole segment
        String bottom = topNString.peek();
        String test = isTop ? stats.getMax() : stats.getMin();
        return test.compareTo(bottom) * (isTop ? 1 : -1) < 0;
    }

    @Override
    protected boolean canProcessInBulk() {
        return true;
    }

    @Override
    protected void bulkTransform(AbstractVector.SegmentInfo segmentInfo,
                                 boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                 boolean useResolution, double resolution,
                                 IQueryState queryState) throws DataSpaceException {
        // support filtering
        int startIndex = vectorSegmentSize*segmentInfo.getSegmentNumber();
        FilteredQueryHelper helper = useFiltering ? new FilteredQueryHelper(startIndex, nextSelectedIndex, positionIterator) : null;

        switch (baseType) {
            case FLOAT:
                bulkTransform((FloatSegment) (segmentInfo.getSegment()), helper);
                break;
            case DOUBLE: 
                bulkTransform((DoubleSegment) (segmentInfo.getSegment()), helper);
                break;
            case BYTE: 
                bulkTransform((ByteSegment) (segmentInfo.getSegment()), helper);
                break;
            case SHORT: 
                bulkTransform((ShortSegment) (segmentInfo.getSegment()), helper);
                break;
            case INTEGER: 
                bulkTransform((IntegerSegment) (segmentInfo.getSegment()), helper);
                break;
            case LONG: 
                bulkTransform((LongSegment) (segmentInfo.getSegment()), helper);
                break;
            case STRING: 
                bulkTransform((StringSegment) (segmentInfo.getSegment()), helper);
                break;
            case BOOLEAN:  
            default:
                throw new DataSpaceException("Don't know how to do TopN for "+baseType);
        }
    }

    private void bulkTransform(FloatSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        float[] data = ((FloatSegmentBackingArray)segment.getBackingArray()).data;
        for (float value: data) {
            handleLong(round(value), (double)value, helper);
        }
    }

    private void bulkTransform(DoubleSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        double[] data = ((DoubleSegmentBackingArray)segment.getBackingArray()).data;
        for (double value: data) {
            handleLong(round(value), value, helper);
        }
    }
    
    private void bulkTransform(LongSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        long[] data = ((LongSegmentBackingArray)segment.getBackingArray()).data;
        for (long value: data) {              
            handleLong(value, null, helper);
        }
    }

    private void bulkTransform(IntegerSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        int[] data = ((IntegerSegmentBackingArray)segment.getBackingArray()).data;
        for (int value: data) {
            handleLong(value, null, helper);
        }
    }

    private void bulkTransform(ShortSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        short[] data = ((ShortSegmentBackingArray)segment.getBackingArray()).data;
        for (short value: data) {
            handleLong(value, null, helper);
        }
    }

    private void bulkTransform(ByteSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        byte[] data = ((ByteSegmentBackingArray)segment.getBackingArray()).data;
        for (byte value: data) {
            handleLong(value, null, helper);
        }
    }

    private void handleLong(long value, Double actual, FilteredQueryHelper helper) throws DataSpaceException {
        if (helper != null) {
            // we're using filtering so should first make sure that we need to do this
            if (! helper.isNextSelected()) {
                return; // skip this one
            }
        }

        // special case for first element
        if (isFirst) {
            isFirst = false;
            topNLong.add(value);
            longHistogram.put(value, 1);
            addDoubleActual(value, actual);
            return;
        }

        // check whether there's a point in adding/removing etc
        long bottom = topNLong.peek();
        if (isTop) {
            if (bottom <= value || topNLong.size() < n) {
                // value in the top N
                longHistogramAdd(value);
                addDoubleActual(value,actual);
            }
            else {
                // value is below the top N - ignore it
                return;
            }
        }
        else {
            if (bottom >= value || topNLong.size() < n) {
                // value in the bottom N
                longHistogramAdd(value);
                addDoubleActual(value,actual);
            }
            else {
                // value is above the bottom N - ignore it
                return;
            }
        }

        // new value - update the structures if it's new

        if (! topNLong.contains(value)) {
            topNLong.add(value);
            if (topNLong.size()>n) {
                long removed = topNLong.poll();
                longHistogram.remove(removed);
                removeDoubleActual(removed, actual);
            }
        }
    }

    private void longHistogramAdd(long value) {
        if (longHistogram.containsKey(value)) {
            longHistogram.put(value, longHistogram.get(value) + 1);
        }
        else {
            longHistogram.put(value,1);
        }
    }

    private void stringHistogramAdd(String value) {
        if (stringHistogram.containsKey(value)) {
            stringHistogram.put(value, stringHistogram.get(value)+1);
        }
        else {
            stringHistogram.put(value,1);
        }
    }

    private void removeDoubleActual(long rounded, Double actual) {
        if (actual!=null) {
            doubleValues.remove(rounded);
        }
    }

    private void addDoubleActual(long rounded, Double actual) {
        if (actual!=null) {
            doubleValues.put(rounded,actual);
        }
    }

    private void bulkTransform(StringSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        int[] data = ((StringSegmentBackingArray)segment.getBackingArray()).data;
        for (int value: data) {
            if (helper != null) {
                // we're using filtering so should first make sure that we need to do this
                if (! helper.isNextSelected()) {
                    continue; // skip this one
                }
            }

            String stringValue = dataSpace.decodeToString(value);

            // special case for first element
            if (isFirst) {
                isFirst = false;
                topNString.add(stringValue);
                stringHistogram.put(stringValue, 1);
                continue;
            }

            // check whether there's a point in adding/removing etc
            String bottom = topNString.peek();
            if (isTop) {
                if (bottom.compareTo(stringValue)<=0 /*bottom <= value*/ || topNString.size() < n) {
                    // value in the top N
                    stringHistogramAdd(stringValue);
                }
                else {
                    // value is below the top N - ignore it
                    continue;
                }
            }
            else {
                if (bottom.compareTo(stringValue) >= 0 /*bottom >= value*/ || topNString.size() < n) {
                    // value in the bottom N
                    stringHistogramAdd(stringValue);
                }
                else {
                    // value is above the bottom N - ignore it
                    continue;
                }
            }

            // new value - update the structures if it's new

            if (! topNString.contains(stringValue)) {
                topNString.add(stringValue);
                if (topNString.size()>n) {
                    String removed = topNString.poll();
                    stringHistogram.remove(removed);
                }
            }
        }
    }

    @Override
    protected ArrayList<Boolean> makeBooleanBins(ArrayList<Integer> count) throws DataSpaceException {
        throw new DataSpaceException("Don't know how to do TopN for "+baseType);
    }
}
