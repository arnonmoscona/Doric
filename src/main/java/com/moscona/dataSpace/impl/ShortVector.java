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
import com.moscona.dataSpace.impl.segment.LongSegmentStats;
import com.moscona.dataSpace.impl.segment.ShortSegment;
import com.moscona.dataSpace.impl.segment.ShortSegmentBackingArray;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created: 12/14/10 4:53 PM
 * By: Arnon Moscona
 */
public class ShortVector extends AbstractVector<Numeric<Short>> {
    private static final long serialVersionUID = 6762424073316008448L;
    ShortSegment lastCreatedSegment;

    public ShortVector(DataSpace dataSpace) {
        super(dataSpace);
        lastCreatedSegment = null;
    }

    public ShortVector(short[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Short value: values) {
            append(value, true);
        }
    }

    public ShortVector(Short[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Short value: values) {
            append(value, true);
        }
    }

    public ShortVector(Numeric<Short>[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        append(values);
    }

    public ShortVector(List<Numeric<Short>> values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for(Numeric<Short> value: values) {
            append(value);
        }
    }

    /**
     * Creates a new vector in the same data space
     *
     * @return
     */
    @Override
    protected AbstractVector<Numeric<Short>> createNew() {
        return new ShortVector(getDataSpace());
    }

    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     *
     * @return
     */
    @Override
    public BaseType getBaseType() {
        return BaseType.SHORT;
    }

    public final void append(short[] values) throws DataSpaceException {
        for(Short value: values) {
            append(value, true);
        }
    }

    public final void append(Short[] values) throws DataSpaceException {
        for(Short value: values) {
            append(value, true);
        }
    }

    public final void append(short value, int numberOfTimes) throws DataSpaceException {
        for (int i=0; i<numberOfTimes; i++) {
            append(value);
        }
    }

    /**
     * Appends the value and increments the vector size
     * @param value
     * @throws DataSpaceException
     */
    public void append(short value) throws DataSpaceException {
        append(value, true);
    }

    /**
     * Appends the value and optionally increments the vector size
     * @param value
     * @param incrementSize
     * @throws DataSpaceException
     */
    protected final void append(short value, boolean incrementSize) throws DataSpaceException {
        ShortSegment segment = (ShortSegment) getSegmentForIndex(size(),true);
        segment.append(value);
        if (incrementSize) {
            incrementSize();
        }
    }

    /**
     * For subclasses to fill in the blank - create a new segment as appropriate
     *
     * @return
     */
    @Override
    protected IVectorSegment<Numeric<Short>> createNewSegment() {
        lastCreatedSegment = new ShortSegment(getDataSpace(), getPersistenceType(), getSegmentSize());
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
    protected void abstractAppend(Numeric<Short> element) throws DataSpaceException {
        ShortSegment segment = (ShortSegment) getSegmentForIndex(size(),true);
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
    protected Numeric<Short> get(IVectorSegment segment, int segmentIndex) {
        Short value = ((ShortSegmentBackingArray)((ShortSegment)segment).getBackingArray()).data[segmentIndex];
        return new Numeric<Short>(value);
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
        // HOLD (fix before release)  support casting from other integral types #IT-492
        short[] src = ((ShortSegmentBackingArray)((ShortSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array
        ShortSegment ShortSegment = (ShortSegment) workingSegment;
        short[] dst = ((ShortSegmentBackingArray) ShortSegment.getBackingArray()).data;
        System.arraycopy(src,pos,dst,start,length);
        ShortSegment.incSize(length);
    }

    public LongSegmentStats getDescriptiveStats() throws DataSpaceException {
        requireSealedStatus(true);
        return (LongSegmentStats) getStats().getDescriptiveStats();
    }  

    /**
     * Produces a query result as a list of the base type of the vector
     * @param bitmap
     * @return
     */
    public List<Short> getMatchingBaseValues(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<Short> retval = new ArrayList<Short>();
        BitmapVectorIterator<Numeric<Short>> iterator = (BitmapVectorIterator<Numeric<Short>>) iterator(bitmap);
        while (iterator.hasNext()) {
            retval.add(iterator.next().getValue());
        }
        return retval;
    }

    public List<Number> asNumbers() throws DataSpaceException {
        ArrayList<Number> list = new ArrayList<Number>(size());
        for (Numeric<Short> num: asList()) {
            list.add(num.getValue());
        }
        return list;
    }

    @Override
    public LongSegmentStats getDescriptiveStats(IBitMap selection) throws DataSpaceException {
        if (selection==null) {
            return getDescriptiveStats();
        }
        // naive implementation
        LongSegmentStats stats = new LongSegmentStats(PersistenceType.TEMPORARY);
        BitmapVectorIterator<Numeric<Short>> iterator = (BitmapVectorIterator<Numeric<Short>>) iterator(selection);
        while (iterator.hasNext()) {
            stats.add(iterator.next().getLongValue());
        }
        return stats;
    }
}
