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

package com.moscona.dataSpace.impl;

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.segment.AbstractSegmentStats;
import com.moscona.dataSpace.impl.segment.BooleanSegmentBackingArray;
import com.moscona.dataSpace.impl.segment.BooleanSegmentStats;
import com.moscona.dataSpace.impl.segment.LogicalSegment;
import com.moscona.dataSpace.util.CompressedBitMap;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.exceptions.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created: 12/14/10 2:47 PM
 * By: Arnon Moscona
 */
public class LogicalVector extends AbstractVector<Logical> {
    private static final long serialVersionUID = -7351341449924401873L;
    LogicalSegment lastCreatedSegment;
    private int trueCount=0;

    public LogicalVector(DataSpace dataSpace) {
        super(dataSpace);
        lastCreatedSegment = null;
    }

    public LogicalVector(boolean[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (boolean value: values) {
            append(value, true);
        }
    }

    public LogicalVector(Boolean[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        append(values);
    }

    public LogicalVector(List<Boolean> values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for(Boolean value: values) {
            append(new Logical(value));
        }
    }

    /**
     * Creates a new vector in the same data space
     *
     * @return
     */
    @Override
    protected AbstractVector<Logical> createNew() {
        return new LogicalVector(getDataSpace());
    }

    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     *
     * @return
     */
    @Override
    public BaseType getBaseType() {
        return BaseType.BOOLEAN;
    }

    public final void append(Boolean[] values) throws DataSpaceException {
        for(Boolean value: values) {
            append(value, true);
        }
    }

    public final void append(boolean value, int numberOfTimes) throws DataSpaceException {
        for (int i=0; i<numberOfTimes; i++) {
            append(value);
        }
    }

    /**
     * Appends the value and increments the vector size
     * @param value
     * @throws DataSpaceException
     */
    public void append(boolean value) throws DataSpaceException {
        append(value, true);
    }

    /**
     * Appends the value and optionally increments the vector size
     * @param value
     * @param incrementSize
     * @throws DataSpaceException
     */
    protected final void append(boolean value, boolean incrementSize) throws DataSpaceException {
        LogicalSegment segment = (LogicalSegment) getSegmentForIndex(size(),true);
        segment.append(value);
        if (incrementSize) {
            incrementSize();
            incrementTrueCount(value);
        }
    }

    private void incrementTrueCount(boolean value) {
        if(value) {
            trueCount++;
        }
    }

    /**
     * For subclasses to fill in the blank - create a new segment as appropriate
     *
     * @return
     */
    @Override
    protected IVectorSegment<Logical> createNewSegment() {
        lastCreatedSegment = new LogicalSegment(getDataSpace(), getPersistenceType(), getSegmentSize());
        return lastCreatedSegment;
    }

    /**
     * Does the append in a subclass using primitive types as appropriate
     * Overall vector size is accumulated in the abstract base class.
     *
     * @param element
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    protected void abstractAppend(Logical element) throws DataSpaceException {
        LogicalSegment segment = (LogicalSegment) getSegmentForIndex(size(),true);
        segment.append(element.getValue());
    }

    /**
     * tells us whether the specific type has stats (e.g. Logical does not)
     *
     * @return
     */
    @Override
    protected boolean vectorTypeSupportsStats() {
        return true;
    }

    /**
     * gets the value in a specific segment after it has been required
     *
     * @param segment
     * @param segmentIndex
     * @return
     */
    @Override
    protected Logical get(IVectorSegment segment, int segmentIndex) {
        boolean value = ((BooleanSegmentBackingArray)((LogicalSegment)segment).getBackingArray()).data[segmentIndex];
        return new Logical(value);
    }

    /**
     * efficient copy of a partial segment
     *
     * @param sourceSegment
     * @param pos
     * @param workingSegment
     * @param start
     * @param length
     */
    @Override
    protected void copyPartialSegment(IVectorSegment sourceSegment, int pos, IVectorSegment workingSegment, int start, int length) throws DataSpaceException {
        boolean[] src = ((BooleanSegmentBackingArray)((LogicalSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array
        LogicalSegment LogicalSegment = (LogicalSegment) workingSegment;
        boolean[] dst = ((BooleanSegmentBackingArray) LogicalSegment.getBackingArray()).data;
        System.arraycopy(src,pos,dst,start,length);
        LogicalSegment.incSize(length);
    }

    public double getTrueRatio() throws DataSpaceException {
        requireSealedStatus(true);
        if (size()==0) {
            throw new DataSpaceException("Cannot calculate true ratio on an empty vector");
        }
        return ((double)trueCount)/size();
    }

    public int getTrueCount() throws DataSpaceException {
        requireSealedStatus(true);
        return trueCount;
    }

    /**
     * Produces a query result as a list of the base type of the vector
     * @param bitmap
     * @return
     */
    public List<Boolean> getMatchingBaseValues(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<Boolean> retval = new ArrayList<Boolean>();
        BitmapVectorIterator<Logical> iterator = (BitmapVectorIterator<Logical>) iterator(bitmap);
        while (iterator.hasNext()) {
            retval.add(iterator.next().getValue());
        }
        return retval;
    }

    @Override
    public IDescriptiveStats<?> getDescriptiveStats(IBitMap selection) throws DataSpaceException, NotImplementedException {
        throw new NotImplementedException("Descriptive stats not meaningful in this context");
    }

    /**
     * This is really a specialized query
     * @return
     * @throws DataSpaceException
     */
    public IBitMap asBitmap() throws DataSpaceException {
        requireSealedStatus(true);
        CompressedBitMap bitMap = new CompressedBitMap();
        if (trueCount==0) {
            return bitMap; // nothing to do
        }

        SegmentIterator iterator = segmentIterator();
        while (iterator.hasNext()) {
            SegmentInfo segmentInfo = iterator.next();
            BooleanSegmentStats stats = (BooleanSegmentStats) segmentInfo.getStats();
            boolean max = stats.getMax();
            if (!max) {
                // no true values in this segment
                for (int i=0; i<stats.getCount(); i++) {
                    bitMap.add(false);
                }
                continue;
            }

            LogicalSegment segment = (LogicalSegment) segmentInfo.getSegment();
            segment.require();
            try {
                for (boolean value: ((BooleanSegmentBackingArray) segment.getBackingArray()).data) {
                    bitMap.add(value);
                }
            }
            finally {
                segment.release();
            }
        }

        return bitMap;
    }
}
