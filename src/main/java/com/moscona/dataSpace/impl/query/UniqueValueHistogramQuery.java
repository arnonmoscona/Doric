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

/**
 * Created: 1/4/11 11:11 AM
 * By: Arnon Moscona
 * Produces a histogram of a vector using its unique values (up to a limit).
 * At the time of initial implementation there is not variant that takes a bitmap. To make a histogram using a bitmap
 * produce a subset based on the bitmap and then do the histogram on the subset.
 */
@SuppressWarnings({"OverlyCoupledMethod", "OverlyCoupledClass"})
public class UniqueValueHistogramQuery<T extends IScalar> extends AbstractHistogramBasedTransformer<T> {
    public static final int MAX_SIZE = 10000;
    private boolean isFirst = true;

    public UniqueValueHistogramQuery() {
        super();
    }

    @Override
    protected void initializeTransformation(IVector<T> vector) throws DataSpaceException {
        dataSpace = vector.getDataSpace();
        baseType = vector.getBaseType();
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                resolution = ((INumericResolutionSupport)vector).getResolution();
                break;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
            case STRING:
            case BOOLEAN:
                break;
            default:
                throw new DataSpaceException("Don't know how to make a histogram for for "+baseType);
        }
    }

    @Override
    protected void incrementString(String bin, int count) throws DataSpaceException {
        super.incrementString(bin, count);
        if (stringHistogram.size()>MAX_SIZE) {
            throw new DataSpaceException("Exceeded maximum allowed size of histogram of "+MAX_SIZE);
        }
    }

    @Override
    protected void incrementLong(long bin, int count) throws DataSpaceException {
        super.incrementLong(bin, count);
        if (stringHistogram.size()>MAX_SIZE) {
            throw new DataSpaceException("Exceeded maximum allowed size of histogram of "+MAX_SIZE);
        }
    }

    @Override
    protected boolean quickTransform(ISegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                     boolean useResolution, double resolution,
                                     IQueryState queryState) throws DataSpaceException {
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                return realQuickTransform((DoubleSegmentStats)stats, segmentNumber, useFiltering, nextSelectedIndex, positionIterator);
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return longQuickTransform((LongSegmentStats)stats, segmentNumber, useFiltering, nextSelectedIndex, positionIterator);
            case STRING:
                return stringQuickTransform((StringSegmentStats)stats, segmentNumber, useFiltering, nextSelectedIndex, positionIterator);
            case BOOLEAN:
                return booleanQuickTransform((BooleanSegmentStats) stats, segmentNumber, useFiltering, nextSelectedIndex, positionIterator);
            default:
                throw new DataSpaceException("Don't know how to make a histogram for "+baseType);
        }
    }

    private boolean realQuickTransform(DoubleSegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        Double actualMin = stats.getMin();
        long min = round(actualMin);
        long max = round(stats.getMax());
        if (min==max) {
            // the entire segment has only one value
            if (!useFiltering) {
                incrementLong(min, stats.getCount());
            }
            else {
                incrementLong(min, countSelectedElementsInSegment(segmentNumber, stats.getCount() , nextSelectedIndex, positionIterator));
            }
            doubleValues.put(min, actualMin);
            return true;
        }
        return false;
    }

    private boolean longQuickTransform(LongSegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        long min = stats.getMin();
        long max = stats.getMax();
        if (min==max) {
            // the entire segment has only one value
            if (!useFiltering) {
                incrementLong(min, stats.getCount());
            }
            else {
                incrementLong(min, countSelectedElementsInSegment(segmentNumber, stats.getCount() , nextSelectedIndex, positionIterator));
            }
            return true;
        }
        return false;
    }

    private boolean stringQuickTransform(StringSegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        String min = stats.getMin();
        String max = stats.getMax();
        if (min.equals(max)) {
            // the entire segment has only one value
            if (!useFiltering) {
                incrementString(min, stats.getCount());
            }
            else {
                incrementString(min, countSelectedElementsInSegment(segmentNumber, stats.getCount() , nextSelectedIndex, positionIterator));
            }
            return true;
        }
        return false;
    }

    private boolean booleanQuickTransform(BooleanSegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        boolean min = stats.getMin();
        boolean max = stats.getMax();
        if (min==max) {
            // the entire segment has only one value
            if (!useFiltering) {
                incrementLong(min?1L:0L, stats.getCount());
            }
            else {
                incrementLong(min?1L:0L, countSelectedElementsInSegment(segmentNumber, stats.getCount() , nextSelectedIndex, positionIterator));
            }
            return true;
        }
        return false;
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
                bulkTransform((LogicalSegment) (segmentInfo.getSegment()), helper);
                break;
            default:
                throw new DataSpaceException("Don't know how to make a histogram for "+baseType);
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

    private void bulkTransform(LogicalSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        boolean[] data = ((BooleanSegmentBackingArray)segment.getBackingArray()).data;
        for (boolean value: data) {
            handleLong(value?1L:0L, null, helper);
        }
    }

    private void handleLong(long bin, Double actual, FilteredQueryHelper helper) throws DataSpaceException {
        if (helper != null) {
            // we're using filtering so should first make sure that we need to do this
            if (! helper.isNextSelected()) {
                return; // skip this one
            }
        }

        incrementLong(bin,1);
        if (actual != null) {
            doubleValues.put(bin,actual);
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

            incrementString(dataSpace.decodeToString(value),1);
        }
    }

}
