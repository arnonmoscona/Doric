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

import com.moscona.exceptions.InvalidArgumentException;
import com.moscona.util.monitoring.stats.DoubleSampleAccumulator;
import com.moscona.util.ISimpleDescriptiveStatistic;
import com.moscona.util.monitoring.stats.LongSampleAccumulator;
import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.query.support.AbstractHistogramBasedTransformer;
import com.moscona.dataSpace.impl.segment.*;

import java.util.ArrayList;

/**
 * Created: 1/20/11 10:55 AM
 * By: Arnon Moscona
 * Makes a running summary. The produces histogram has additional columns "first" (open) and "last" (close)
 * for each bin.
 */
public class RunningOLHCSummaryHistogramQuery<T extends IScalar> extends AbstractHistogramBasedTransformer<T> {
    private ArrayList<Integer> periods;
    private ArrayList<ISimpleDescriptiveStatistic> bins=null; // collects the bins
    private DoubleSampleAccumulator currentDoubleStats=null;
    private LongSampleAccumulator currentLongStats=null;
    private int binSize;
    private IPositionIterator bitMapIterator=null;
    private int currentIndex = -1 ;
    private int nextTrue = -1; // the next index to consider true
    private int cardinality=0;
    private int collectedCount=0;
    private boolean hasNext=true;
    private String[] requestedSummaries = null;

    /**
     * Creates an OLHC histogram query with a specified bin size (running summary on all vector elements switching
     * bins every binSize entries)
     * @param binSize
     * @param requestedSummaries - which columns are requested
     */
    public RunningOLHCSummaryHistogramQuery(int binSize, String[] requestedSummaries) throws DataSpaceException, InvalidArgumentException {
        super();
        if (requestedSummaries != null) {
            for (String col: requestedSummaries) {
                OLHCHistogram.validateSummaryColumnName(col);
            }
            this.requestedSummaries = requestedSummaries.clone();
        }
        periods = new ArrayList<Integer>();
        validateBinSize(binSize);
        this.binSize = binSize;
        nextTrue = 0;
    }

    public RunningOLHCSummaryHistogramQuery(int binSize) throws DataSpaceException, InvalidArgumentException {
        this(binSize, (String[]) null);
    }

    /**
     * Creates an OLHC histogram query with a specified bin size and a selection bitmap
     * (running summary on vector elements switching bins every binSize selected entries)
     * Selected entries are those entries for which the bitmap it true
     * @param binSize
     * @param selection
     */
    public RunningOLHCSummaryHistogramQuery(int binSize, IBitMap selection, String[] requestedSummaries) throws DataSpaceException, InvalidArgumentException {
        this(binSize);
        setSelection(selection);
    }

    @Override
    protected void setSelection(IBitMap selection) throws DataSpaceException {
        this.selection = selection;
        if (selection==null) {
            return;
        }
        this.bitMapIterator = selection.getPositionIterator();
        nextTrue = -1;
        cardinality = selection.cardinality();
        updateNextTrue();
    }

    @Override
    protected boolean abstractTransformerShouldIgnoreSelection() {
        return true;
    }

    public RunningOLHCSummaryHistogramQuery(int binSize, IBitMap selection) throws DataSpaceException, InvalidArgumentException {
        this(binSize, selection, null);
    }


    private void updateNextTrue() throws DataSpaceException {
        if (bitMapIterator == null) {
            nextTrue++;
        }
        else {
            if (bitMapIterator.hasNext()) {
                nextTrue = bitMapIterator.next();
            }
            else {
                hasNext = false;
            }
        }
    }

    private void validateBinSize(int binCount) throws DataSpaceException {
        if (binCount<1) {
            throw new DataSpaceException("Bin count must be at least 1. Got "+binCount);
        }
    }


    @Override
    protected void initializeTransformation(IVector<T> vector) throws DataSpaceException {
        collectedCount = 0;
        currentIndex = -1 ;
        hasNext=true;
        periods = new ArrayList<Integer>();

        try {
            baseType = vector.getBaseType();
            dataSpace = vector.getDataSpace();

            IVectorStats<T> stats = vector.getStats();
            if (selection==null) {
                cardinality = vector.size();
            }
            bins = new ArrayList<ISimpleDescriptiveStatistic>();

            switch (baseType) {
                case FLOAT:
                case DOUBLE:
                    currentDoubleStats = new DoubleSampleAccumulator();
                    break;
                case BYTE:
                case SHORT:
                case INTEGER:
                case LONG:
                    currentLongStats = new LongSampleAccumulator();
                    break;
                case STRING:
                case BOOLEAN:
                default:
                    throwIncompatibleType();
            }
        }
        catch (ClassCastException e) {
            throw new DataSpaceException("ClassCastException while initializing "+this.getClass().getSimpleName()+" baseType="+baseType);
        }
    }

    private void throwIncompatibleType() throws DataSpaceException {
        throw new DataSpaceException("Don't know how to make an OLHC histogram for for "+baseType);
    }



    @Override
    protected boolean quickTransform(ISegmentStats stats, int segmentNumber,
                                     boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                     boolean useResolution, double resolution,
                                     IQueryState queryState) throws DataSpaceException {
        int segmentLength = stats.getCount();
        if (cardinality==0 || !hasNext || nextTrue>currentIndex+segmentLength) {
            // this can happen when we use a bitmap and the next true value is beyond this segment (or there is no next true value)
            currentIndex += segmentLength;
            return true;  // skip this segment
        }
        return false; // cannot do a quick transform - must iterate over all
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
        switch(baseType) {
            case FLOAT:
                bulkTransform((FloatSegment) (segmentInfo.getSegment()));
                break;
            case DOUBLE:
                bulkTransform((DoubleSegment) (segmentInfo.getSegment()));
                break;
            case BYTE:
                bulkTransform((ByteSegment) (segmentInfo.getSegment()));
                break;
            case SHORT:
                bulkTransform((ShortSegment) (segmentInfo.getSegment()));
                break;
            case INTEGER:
                bulkTransform((IntegerSegment) (segmentInfo.getSegment()));
                break;
            case LONG:
                bulkTransform((LongSegment) (segmentInfo.getSegment()));
                break;
            case STRING:
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }
    }

    private void newDoubleBin() {
        bins.add(currentDoubleStats);
        currentDoubleStats = new DoubleSampleAccumulator();
        periods.add(collectedCount);
    }

    private void newLongBin() {
        bins.add(currentLongStats);
        currentLongStats = new LongSampleAccumulator();
        periods.add(collectedCount);
    }

    private void throwIndexOutOfRange(int i) throws DataSpaceException {
        throw new DataSpaceException("Bug! After calculating index into segment: "+i+" got an index that is not in this segment!");
    }

    private void addDataPoint(double n) throws DataSpaceException {
        if (crossingBinBoundary()) {
            newDoubleBin();
        }

        currentDoubleStats.addSample(n);
        incrementCollectedCount();

        if (noMoreItems()) {
            newDoubleBin();
        }
    }

    private void addDataPoint(long n) throws DataSpaceException {
        if (crossingBinBoundary()) {
            newLongBin();
        }

        currentLongStats.addSample(n);
        incrementCollectedCount();

        if (noMoreItems()) {
            newLongBin();
        }
    }

    private boolean noMoreItems() {
        return !hasNext || collectedCount==cardinality;
    }

    private void incrementCollectedCount() throws DataSpaceException {
        collectedCount++;
        updateNextTrue();
    }

    private boolean crossingBinBoundary() throws DataSpaceException {
        if(collectedCount>=cardinality) {
            throw new DataSpaceException("Bug! collectedCount can never exceed cardinality! collectedCount="+collectedCount+", cardinality="+cardinality);
        }
        return collectedCount>0 && (collectedCount)%binSize==0;
    }

    private int nextIndex(int maxI, int startingIndex) throws DataSpaceException {
        int i = nextTrue - startingIndex; // relative index into this segment
        if (i>maxI) {
            throwIndexOutOfRange(i);
        }
        currentIndex = nextTrue;
        return i;
    }

    private void bulkTransform(FloatSegment segment) throws DataSpaceException {
        float[] data = ((FloatSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    private void bulkTransform(DoubleSegment segment) throws DataSpaceException {
        double[] data = ((DoubleSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    private void bulkTransform(LongSegment segment) throws DataSpaceException {
        long[] data = ((LongSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    private void bulkTransform(IntegerSegment segment) throws DataSpaceException {
        int[] data = ((IntegerSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    private void bulkTransform(ShortSegment segment) throws DataSpaceException {
        short[] data = ((ShortSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    private void bulkTransform(ByteSegment segment) throws DataSpaceException {
        byte[] data = ((ByteSegmentBackingArray)segment.getBackingArray()).data;
        int stopIndex = segment.startingIndex()+data.length-1;

        while (hasNext && nextTrue <= stopIndex && collectedCount < cardinality) {
            int i = nextIndex(data.length-1, segment.startingIndex());
            addDataPoint(data[i]);
        }
    }

    @SuppressWarnings({"unchecked", "OverlyLongMethod"})
    @Override
    protected Histogram finishTransformation() throws DataSpaceException {
        switch (baseType) {
            case FLOAT:
            case DOUBLE:
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return new OLHCHistogram(dataSpace, bins, periods, requestedSummaries);
            case STRING:
            case BOOLEAN:
            default:
                throwIncompatibleType();
        }
        throw new DataSpaceException("Bug! Should not have gotten here...");
    }


}
