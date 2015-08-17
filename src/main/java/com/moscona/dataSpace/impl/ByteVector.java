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
import com.moscona.dataSpace.impl.segment.ByteSegment;
import com.moscona.dataSpace.impl.segment.ByteSegmentBackingArray;
import com.moscona.dataSpace.impl.segment.LongSegmentStats;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created: 12/14/10 5:36 PM
 * By: Arnon Moscona
 */
public class ByteVector extends AbstractVector<Numeric<Byte>> {
    private static final long serialVersionUID = -971765861710017336L;
    ByteSegment lastCreatedSegment;

    public ByteVector(DataSpace dataSpace) {
        super(dataSpace);
        lastCreatedSegment = null;
    }

    public ByteVector(byte[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Byte value: values) {
            append(value, true);
        }
    }

    public ByteVector(Byte[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Byte value: values) {
            append(value, true);
        }
    }

    public ByteVector(Numeric<Byte>[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        append(values);
    }

    public ByteVector(List<Numeric<Byte>> values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for(Numeric<Byte> value: values) {
            append(value);
        }
    }

    /**
     * Creates a new vector in the same data space
     *
     * @return
     */
    @Override
    protected AbstractVector<Numeric<Byte>> createNew() {
        return new ByteVector(getDataSpace());
    }

    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     *
     * @return
     */
    @Override
    public BaseType getBaseType() {
        return BaseType.BYTE;
    }

    public final void append(byte[] values) throws DataSpaceException {
        for(Byte value: values) {
            append(value, true);
        }
    }

    public final void append(Byte[] values) throws DataSpaceException {
        for(Byte value: values) {
            append(value, true);
        }
    }

    public final void append(byte value, int numberOfTimes) throws DataSpaceException {
        for (int i=0; i<numberOfTimes; i++) {
            append(value);
        }
    }

    /**
     * Appends the value and increments the vector size
     * @param value
     * @throws DataSpaceException
     */
    public void append(byte value) throws DataSpaceException {
        append(value, true);
    }

    /**
     * Appends the value and optionally increments the vector size
     * @param value
     * @param incrementSize
     * @throws DataSpaceException
     */
    protected final void append(byte value, boolean incrementSize) throws DataSpaceException {
        ByteSegment segment = (ByteSegment) getSegmentForIndex(size(),true);
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
    protected IVectorSegment<Numeric<Byte>> createNewSegment() {
        lastCreatedSegment = new ByteSegment(getDataSpace(), getPersistenceType(), getSegmentSize());
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
    protected void abstractAppend(Numeric<Byte> element) throws DataSpaceException {
        ByteSegment segment = (ByteSegment) getSegmentForIndex(size(),true);
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
    protected Numeric<Byte> get(IVectorSegment segment, int segmentIndex) {
        Byte value = ((ByteSegmentBackingArray)((ByteSegment)segment).getBackingArray()).data[segmentIndex];
        return new Numeric<Byte>(value);
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
        byte[] src = ((ByteSegmentBackingArray)((ByteSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array
        ByteSegment ByteSegment = (ByteSegment) workingSegment;
        byte[] dst = ((ByteSegmentBackingArray) ByteSegment.getBackingArray()).data;
        System.arraycopy(src,pos,dst,start,length);
        ByteSegment.incSize(length);
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
    public List<Byte> getMatchingBaseValues(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<Byte> retval = new ArrayList<Byte>();
        BitmapVectorIterator<Numeric<Byte>> iterator = (BitmapVectorIterator<Numeric<Byte>>) iterator(bitmap);
        while (iterator.hasNext()) {
            retval.add(iterator.next().getValue());
        }
        return retval;
    }

    public List<Number> asNumbers() throws DataSpaceException {
        ArrayList<Number> list = new ArrayList<Number>(size());
        for (Numeric<Byte> num: asList()) {
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
        BitmapVectorIterator<Numeric<Byte>> iterator = (BitmapVectorIterator<Numeric<Byte>>) iterator(selection);
        while (iterator.hasNext()) {
            stats.add(iterator.next().getLongValue());
        }
        return stats;
    }
}
