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
import com.moscona.dataSpace.impl.query.support.AbstractHistogramBasedTransformer;
import com.moscona.dataSpace.impl.segment.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created: 1/4/11 5:44 PM
 * By: Arnon Moscona
 * A query that takes a range of values and a number of bins and makes a histogram
 */
public class RangeHistogramQuery<T extends IScalar> extends AbstractHistogramBasedTransformer<T> {
    public static final int MAX_AUTO_BINS = 21;
    private boolean autoBins = true;
    private boolean autoRange = true;
    private double minDouble = 0.0;
    private double maxDouble = 0.0;
    private long minLong = 0L;
    private long maxLong = 0L;
    private int binCount = 0;
    private long[] longBinBoundary = null;
    private double[] doubleBinBoundary = null;
    private int[] binCounters;
    private long binSizeLong=0L;
    private double binSizeDouble=0.0;

    private boolean clipRange = false;

    /**
     * Creates a histogram query with a specified number of bins from min to max.
     * Applies to all numeric vectors. Clips data outside the range.
     * @param binCount
     * @param min
     * @param max
     * @param collectDescriptiveStats if true then the query will also produce descriptive stats columns in the histogram
     */
    public RangeHistogramQuery(int binCount, double min, double max, boolean collectDescriptiveStats) throws DataSpaceException {
        super();
        autoBins = false;
        autoRange = false;
        this.binCount = binCount;
        minDouble = min;
        maxDouble = max;
        minLong = Math.round(minDouble);
        maxLong = Math.round(maxDouble);

        clipRange = true;
        setCollectBinStats(collectDescriptiveStats);

        validateBinCount(binCount);
        validateMaxMinDouble();
    }

    /**
     * Creates a histogram query with a specified number of bins from min to max.
     * Applies to all numeric vectors. Clips data outside the range.
     * @param binCount
     * @param min
     * @param max
     * @throws DataSpaceException
     */
    public RangeHistogramQuery(int binCount, double min, double max) throws DataSpaceException {
        this(binCount, min, max, false);
    }

    /**
     * Creates a histogram query with a specified number of bins from min to max.
     * Applies to all numeric vectors. Clips data outside the range.
     * @param binCount the requested bin count. If there are clearly not enough possible values, the end bin count can be lower
     * @param min
     * @param max
     * @param collectDescriptiveStats if true then the query will also produce descriptive stats columns in the histogram
     */
    public RangeHistogramQuery(int binCount, long min, long max, boolean collectDescriptiveStats) throws DataSpaceException {
        super();
        autoBins = false;
        autoRange = false;
        this.binCount = binCount;
        minLong = min;
        maxLong = max;
        minDouble = min;
        maxDouble = max;

        clipRange = true;
        setCollectBinStats(collectDescriptiveStats);

        validateBinCount(binCount);
        validateMaxMinLong();
    }

    /**
     * Creates a histogram query with a specified number of bins from min to max.
     * Applies to all numeric vectors. Clips data outside the range.
     * @param binCount
     * @param min
     * @param max
     * @throws DataSpaceException
     */
    public RangeHistogramQuery(int binCount, long min, long max) throws DataSpaceException {
        this(binCount,  min, max, false);
    }

    /**
     * Creates a histogram query with a specified number of bins on the entire vector range.
     * Applies to all numeric vectors.
     * @param binCount
     * @param collectDescriptiveStats if true then the query will also produce descriptive stats columns in the histogram
     */
    public RangeHistogramQuery(int binCount, boolean collectDescriptiveStats) throws DataSpaceException {
        super();
        this.binCount = binCount;
        autoBins = false;
        autoRange = true;
        validateBinCount(binCount);
        setCollectBinStats(collectDescriptiveStats);
    }

    /**
     * Creates a histogram query with a specified number of bins on the entire vector range.
     * Applies to all numeric vectors.
     * @param binCount
     * @throws DataSpaceException
     */
    public RangeHistogramQuery(int binCount) throws DataSpaceException {
        this(binCount,false);
    }

    /**
     * Creates a histogram query with auto-selection of number of bins and range.
     * Applies to all numeric vectors.
     */
    public RangeHistogramQuery() {
        super();
        autoBins = true;
        autoRange = true;
    }

    private void validateMaxMinDouble() throws DataSpaceException {
        if (maxDouble<=minDouble) {
            throw new DataSpaceException("Max must be larger than min. Got "+maxDouble+" and "+minDouble+" respectively");
        }
    }

    private void validateMaxMinLong() throws DataSpaceException {
        if (maxLong<=minLong) {
            throw new DataSpaceException("Max must be larger than min. Got "+maxLong+" and "+minLong+" respectively");
        }
    }

    private void validateBinCount(int binCount) throws DataSpaceException {
        if (binCount<1) {
            throw new DataSpaceException("Bin count must be at least 1. Got "+binCount);
        }
    }


    @Override
    protected void initializeTransformation(IVector<T> vector) throws DataSpaceException {
        try {
            baseType = vector.getBaseType();
            dataSpace = vector.getDataSpace();

            IVectorStats<T> stats = vector.getStats();
            Number min = (Number)stats.getDescriptiveStats().getMin();
            Number max = (Number)stats.getDescriptiveStats().getMax();

            switch (baseType) {
                case FLOAT:
                case DOUBLE:
                    resolution = ((INumericResolutionSupport)vector).getResolution();
                    if (autoRange) {
                        minDouble = min.doubleValue();
                        maxDouble = max.doubleValue();
                        minLong = Math.round(minDouble);
                        maxLong = Math.round(maxDouble);
                    }
                    if (autoBins) {
                        calculateAutoBins(vector.size(), minDouble, maxDouble);
                    }

                    validateBinCount(binCount);
                    validateMaxMinDouble();
                    break;
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                    if (autoRange) {
                        minLong = min.longValue();
                        maxLong = max.longValue();
                        minDouble = minLong;
                        maxDouble = maxLong;
                    }
                    calculateAutoBins(vector.size(), minLong, maxLong); // user request for specific bin counts can be overridden

                    validateBinCount(binCount);
                    validateMaxMinLong();
                    break;
                case STRING:
                case BOOLEAN:
                default:
                    throwIncompatibleType();
            }
            calculateBins();
        }
        catch (ClassCastException e) {
            throw new DataSpaceException("ClassCastException while initializing "+this.getClass().getSimpleName()+" baseType="+baseType);
        }
    }

    /**
     * Calculates the bin boundary array
     */
    private void calculateBins() throws DataSpaceException {
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
                calculateDoubleBoundaries();
                break;
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                calculateLongBoundaries();
                break;
            case STRING:
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }
    }

    private void calculateLongBoundaries() {
        int boundary = 0;
        longBinBoundary = new long[binCount+1];
        binCounters = new int[binCount];
        binSizeLong = Math.max(1L,(maxLong-minLong)/binCount);
        for (long value=minLong; value<=maxLong; value+=binSizeLong) {
            if (boundary>binCount) {
                break;
            }
            longBinBoundary[boundary++] = value;
        }
        longBinBoundary[binCount] = maxLong+1;
    }

    private void calculateDoubleBoundaries() {
        int boundary = 0;
        doubleBinBoundary = new double[binCount+1];
        binCounters = new int[binCount];
        binSizeDouble = (maxDouble-minDouble)/binCount;
        for (double value=minDouble; value<=maxDouble; value+=binSizeDouble) {
            if (boundary>binCount) {
                break;
            }
            doubleBinBoundary[boundary++] = value;
        }
        doubleBinBoundary[binCount] = maxDouble;
    }

    /**
     * Calculates how many bins to make based on the potential number of distinct values in the vector,
     * taking resolution into account
     * @param size
     * @param min
     * @param max
     */
    public void calculateAutoBins(int size, double min, double max) {
        // for vectors with no more than 5 possible distinct values use the number of bins available in the range based on
        // resolution or 5 - whatever the lower is
        long potentialDistinct = Math.round((max-min)/resolution);
        long bins = Math.min(potentialDistinct, size);
        if (bins <= 5) {
            binCount = 5;
            return;
        }

        // for vectors larger than 5 go for 21 bins if possible
        binCount = (int)Math.min(MAX_AUTO_BINS, bins);
    }

    /**
     * Calculates how many bins to make based on the potential number of distinct values in the vector
     * @param size
     * @param min
     * @param max
     */
    public void calculateAutoBins(int size, long min, long max) {
        // remember the user setting (if there was one)
        int originalBinCount = binCount;

        // for vectors with no more than 5 possible distinct values use the number of bins available in the range based on
        // resolution or 5 - whatever the lower is
        long potentialDistinct = max-min+1;
        long bins = Math.min(potentialDistinct, size);
        if (bins <= 5) {
            binCount = (int)bins;
            return;
        }

        // for vectors larger than 5 go for 21 bins if possible
        binCount = (int)Math.min(MAX_AUTO_BINS, bins);

        if (!autoBins) {
            // the user requested a specific bin count
            if (potentialDistinct<originalBinCount) {
                // but there cannot be more than what we calculated - ignore the user request
                binCount = Math.min(binCount, originalBinCount);
            }
            else {
                // honor the user request
                binCount = originalBinCount;
            }
        }
    }

    private void throwIncompatibleType() throws DataSpaceException {
        throw new DataSpaceException("Don't know how to make a range histogram for for "+baseType);
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
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }
        return false;
    }

    private boolean longQuickTransform(LongSegmentStats stats, int segmentNumber,
                                       boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        long min = stats.getMin();
        long max = stats.getMax();
        if (min==max) {
            // the entire segment has only one value
            incrementLong(min, segmentNumber, stats, useFiltering, nextSelectedIndex, positionIterator);
            return true;
        }
        return false;
    }

    private void incrementLong(long value, int segmentNumber, LongSegmentStats stats, boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        if (!useFiltering) {
            incrementLong(value, stats.getCount()); // just falling back on old implementation

            if (shouldCollectStats()) {
                for (int i=0; i<stats.getCount(); i++) {
                    addLongValueToStats(getBin(value), value);
                }
            }
        }
        else {
            // use filtering
            incrementLong(value, countSelectedElementsInSegment(segmentNumber, stats.getCount(), nextSelectedIndex, positionIterator, value), true);
        }
    }

    /**
     * A variant of countSelectedElementsInSegment which also collects a single repeated value into the stats.
     * This is a weird method with serious side effects AND duplicative code. HOLD try to fix this odd situation
     * @param segmentNumber
     * @param thisSegmentSize
     * @param nextSelectedIndex
     * @param positionIterator
     * @param valueForStats
     * @return
     * @throws DataSpaceException
     */
    private int countSelectedElementsInSegment(int segmentNumber, int thisSegmentSize, int nextSelectedIndex, IPositionIterator positionIterator, long valueForStats) throws DataSpaceException {
        int firstIndex = vectorSegmentSize * segmentNumber;
        int lastIndex = firstIndex + thisSegmentSize;
        int counter = 0;
        int testValue = nextSelectedIndex;
        int bin = getBin(valueForStats); // for stats
        while (testValue>=firstIndex && testValue<=lastIndex) {
            counter++;
            addLongValueToStats(bin, valueForStats);  // add to stats
            if (positionIterator.hasNext()) {
                testValue = positionIterator.next();
            }
            else {
                return counter;
            }
        }
        return counter;
    }

    /**
     * A variant of countSelectedElementsInSegment which also collects a single repeated value into the stats.
     * This is a weird method with serious side effects AND duplicative code. HOLD try to fix this odd situation
     * @param segmentNumber
     * @param thisSegmentSize
     * @param nextSelectedIndex
     * @param positionIterator
     * @param valueForStats
     * @return
     * @throws DataSpaceException
     */
    private int countSelectedElementsInSegment(int segmentNumber, int thisSegmentSize, int nextSelectedIndex, IPositionIterator positionIterator, double valueForStats) throws DataSpaceException {
        int firstIndex = vectorSegmentSize * segmentNumber;
        int lastIndex = firstIndex + thisSegmentSize;
        int counter = 0;
        int testValue = nextSelectedIndex;
        int bin = adjustForLastBin(getBin(valueForStats)); // for stats
        while (testValue>=firstIndex && testValue<=lastIndex) {
            counter++;
            addDoubleValueToStats(bin, valueForStats);  // add to stats
            if (positionIterator.hasNext()) {
                testValue = positionIterator.next();
            }
            else {
                return counter;
            }
        }
        return counter;
    }

    private boolean realQuickTransform(DoubleSegmentStats stats, int segmentNumber,
                                       boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        Double actualMin = stats.getMin();
        long min = round(actualMin);
        long max = round(stats.getMax());
        if (min==max) {
            // the entire segment has only one value
            incrementDouble(actualMin, segmentNumber, stats, useFiltering, nextSelectedIndex, positionIterator);
            return true;
        }
        return false;
    }

    private void incrementDouble(Double value, int segmentNumber, DoubleSegmentStats stats, boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        if (!useFiltering) {
            incrementDouble(value, stats.getCount(), false); // just falling back on old implementation
//            if (shouldCollectStats()) {
//                for (int i=0; i<stats.getCount(); i++) {
//                    addDoubleValueToStats(adjustForLastBin(getBin(value)), value);
//                }
//            }
        }
        else {
            incrementDouble(value, countSelectedElementsInSegment(segmentNumber, stats.getCount(), nextSelectedIndex, positionIterator, value), true);
        }
    }

    private void incrementDouble(double value, int count, boolean statsAlreadyCounted) throws DataSpaceException {
        incrementDouble(value,count,null, statsAlreadyCounted);
    }

    private void incrementDouble(double value, int count, FilteredQueryHelper helper, boolean statsAlreadyCounted) throws DataSpaceException {
        if (helper != null) {
            // we're using filtering so should first make sure that we need to do this
            if (! helper.isNextSelected()) {
                return; // skip this one
            }
        }

        int bin = getBin(value);
        if (clipRange && (bin<0 || bin>binCounters.length-1 && Math.abs(value-maxDouble)>resolution)) {
            return;
        }
        binCounters[adjustForLastBin(bin)]+=count;
        if (shouldCollectStats() && !statsAlreadyCounted) {
            for (int i=0; i<count; i++) {
                addDoubleValueToStats(adjustForLastBin(bin), value);
            }
        }
    }

    private int adjustForLastBin(int bin) {
        return Math.min(bin, binCounters.length-1);
    }

    private int getBin(double value) {
        return (int)((value-minDouble)/binSizeDouble);
    }

    protected void incrementLong(long value, int count, boolean statsAlreadyCounted) throws DataSpaceException {
        incrementLong(value, count, null, statsAlreadyCounted);
    }

    protected void incrementLong(long value, int count, FilteredQueryHelper helper, boolean statsAlreadyCounted) throws DataSpaceException {
        if (helper != null) {
            // we're using filtering so should first make sure that we need to do this
            if (! helper.isNextSelected()) {
                return; // skip this one
            }
        }

        int bin = getBin(value);
        if (clipRange && (bin<0 || bin>binCounters.length-1)) {
            return;
        }
        binCounters[adjustForLastBin(bin)]+=count;
        if (shouldCollectStats() && !statsAlreadyCounted) {
            for (int i=0; i<count; i++) {
                addLongValueToStats(adjustForLastBin(bin), value);
            }
        }
    }

    private int getBin(long value) {
        return (int)((value-minLong)/binSizeLong);
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
        FilteredQueryHelper helper = makeFilteredQueryHelper(useFiltering, startIndex, nextSelectedIndex, positionIterator);
        
        switch(baseType) {
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
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }
    }

    private FilteredQueryHelper makeFilteredQueryHelper(boolean useFiltering, int startIndex, int nextSelectedIndex, IPositionIterator positionIterator) {
        return useFiltering ? new FilteredQueryHelper(startIndex, nextSelectedIndex, positionIterator) : null;
    }

    private void bulkTransform(FloatSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        float[] data = ((FloatSegmentBackingArray)segment.getBackingArray()).data;
        for (float value: data) {
            incrementDouble((double) value, 1, helper, false);
        }
    }

    private void bulkTransform(DoubleSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        double[] data = ((DoubleSegmentBackingArray)segment.getBackingArray()).data;
        for (double value: data) {
            incrementDouble(value, 1, helper, false);
        }
    }

    private void bulkTransform(LongSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        long[] data = ((LongSegmentBackingArray)segment.getBackingArray()).data;
        for (long value: data) {
            incrementLong(value, 1, helper, false);
        }
    }

    private void bulkTransform(IntegerSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        int[] data = ((IntegerSegmentBackingArray)segment.getBackingArray()).data;
        for (int value: data) {
            incrementLong(value, 1, helper, false);
        }
    }

    private void bulkTransform(ShortSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        short[] data = ((ShortSegmentBackingArray)segment.getBackingArray()).data;
        for (short value: data) {
            incrementLong(value, 1, helper, false);
        }
    }

    private void bulkTransform(ByteSegment segment, FilteredQueryHelper helper) throws DataSpaceException {
        byte[] data = ((ByteSegmentBackingArray)segment.getBackingArray()).data;
        for (byte value: data) {
            incrementLong(value, 1, helper, false);
        }
    }

    @SuppressWarnings({"unchecked", "OverlyLongMethod"})
    @Override
    protected Histogram finishTransformation() throws DataSpaceException {
        ArrayList<String> names = new ArrayList<String>();
        ArrayList<Integer> count = new ArrayList<Integer>();
        ArrayList minBins = null;
        ArrayList maxBins = null;
        HashMap<Long,Integer> longHistogramMin = makeLongHistogram(true);
        HashMap<Long,Integer> longHistogramMax = makeLongHistogram(false);
        longHistogram = longHistogramMin;

        switch (baseType) {
            case FLOAT:
                minBins = makeFloatBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeFloatBins(new ArrayList<Integer>());
                break;
            case DOUBLE:
                minBins = makeDoubleBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeDoubleBins(new ArrayList<Integer>());
                break;
            case BYTE:
                minBins = makeByteBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeByteBins(new ArrayList<Integer>());
                break;
            case SHORT:
                minBins = makeShortBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeShortBins(new ArrayList<Integer>());
                break;
            case INTEGER:
                minBins = makeIntegerBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeIntegerBins(new ArrayList<Integer>());
                break;
            case LONG:
                minBins = makeLongBins(count);
                longHistogram = longHistogramMax;
                maxBins = makeLongBins(new ArrayList<Integer>());
                break;
            case STRING:
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }

        populateBinNames(names);
        Histogram histogram = new Histogram(dataSpace, names, minBins, maxBins, count); // unchecked call
        if (shouldCollectStats()) {
            appendStatsColumns(histogram);
        }
        return histogram;
    }

    private void populateBinNames(ArrayList<String> names) {
        if (longBinBoundary != null && longBinBoundary.length != 0) {
            // we counted using long boundaries
            for (int i=0; i<binCounters.length; i++) {
                String min = StringHelper.prettyPrint(longBinBoundary[i]);
                String max = StringHelper.prettyPrint(longBinBoundary[i+1]-1);
                names.add(min.equals(max) ? min : min + "-" + max);
            }
        }
        else {
            // we counted using double boundaries
            for (int i=0; i<binCounters.length; i++) {
                String min = StringHelper.prettyPrint(doubleBinBoundary[i]);
                String max = StringHelper.prettyPrint(doubleBinBoundary[i + 1]);
                if (i==binCounters.length-1) {
                    names.add("["+min+"-"+max+"]");
                }
                else {
                    names.add("["+min+"-"+max+")");
                }
            }
        }
    }

    private HashMap<Long,Integer> makeLongHistogram(boolean useMinValues) {
        HashMap<Long,Integer> hist = new HashMap<Long,Integer>();
        if (longBinBoundary != null && longBinBoundary.length != 0) {
            // we counted using long boundaries
            for (int i=0; i<binCounters.length; i++) {
                hist.put(useMinValues?longBinBoundary[i]:longBinBoundary[i+1]-1, binCounters[i]);
            }
        }
        else {
            // we counted using double boundaries
            for (int i=0; i<binCounters.length; i++) {
                double doubleKey = useMinValues ? doubleBinBoundary[i] : doubleBinBoundary[i + 1];
                long key = round(doubleKey);
                doubleValues.put(key, doubleKey);
                hist.put(key, binCounters[i]);
            }
        }
        return hist;
    }
}
