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
import com.moscona.dataSpace.Numeric;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.segment.*;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created: 12/14/10 6:11 PM
 * By: Arnon Moscona
 */
public class FloatVector extends AbstractVector<Numeric<Float>> implements INumericResolutionSupport {
    private static final long serialVersionUID = 8707167273627357193L;
    FloatSegment lastCreatedSegment;

    public FloatVector(DataSpace dataSpace) {
        super(dataSpace);
        lastCreatedSegment = null;
    }

    public FloatVector(float[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Float value: values) {
            append(value, true);
        }
    }

    public FloatVector(Float[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (Float value: values) {
            append(value, true);
        }
    }

    public FloatVector(Numeric<Float>[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        append(values);
    }

    public FloatVector(List<Numeric<Float>> values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for(Numeric<Float> value: values) {
            append(value);
        }
    }

    /**
     * Creates a new vector in the same data space
     *
     * @return
     */
    @Override
    protected AbstractVector<Numeric<Float>> createNew() {
        return new FloatVector(getDataSpace());
    }

    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     *
     * @return
     */
    @Override
    public BaseType getBaseType() {
        return BaseType.FLOAT;
    }

    public final void append(float[] values) throws DataSpaceException {
        for(Float value: values) {
            append(value, true);
        }
    }

    public final void append(Float[] values) throws DataSpaceException {
        for(Float value: values) {
            append(value, true);
        }
    }

    public final void append(float value, int numberOfTimes) throws DataSpaceException {
        for (int i=0; i<numberOfTimes; i++) {
            append(value);
        }
    }

    /**
     * Appends the value and increments the vector size
     * @param value
     * @throws DataSpaceException
     */
    public void append(float value) throws DataSpaceException {
        append(value, true);
    }

    /**
     * Appends the value and optionally increments the vector size
     * @param value
     * @param incrementSize
     * @throws DataSpaceException
     */
    protected final void append(float value, boolean incrementSize) throws DataSpaceException {
        FloatSegment segment = (FloatSegment) getSegmentForIndex(size(),true);
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
    protected IVectorSegment<Numeric<Float>> createNewSegment() {
        lastCreatedSegment = new FloatSegment(getDataSpace(), getPersistenceType(), getSegmentSize());
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
    protected void abstractAppend(Numeric<Float> element) throws DataSpaceException {
        FloatSegment segment = (FloatSegment) getSegmentForIndex(size(),true);
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
    protected Numeric<Float> get(IVectorSegment segment, int segmentIndex) {
        Float value = ((FloatSegmentBackingArray)((FloatSegment)segment).getBackingArray()).data[segmentIndex];
        return new Numeric<Float>(value);
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
        FloatSegment floatSegment = (FloatSegment) workingSegment;
        float[] dst = ((FloatSegmentBackingArray) floatSegment.getBackingArray()).data;
        if (sourceSegment instanceof FloatSegment) {
            float[] src = ((FloatSegmentBackingArray)((FloatSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array
            System.arraycopy(src,pos,dst,start,length);
        }
        else if (sourceSegment instanceof DoubleSegment) {
            // support a down-cast double=>float
            double[] src = ((DoubleSegmentBackingArray)((DoubleSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array

            // must resort to less efficient array copy
            for (int i=0; i< length; i++) {
                dst[start+i] = (float) src[pos+i];
            }
            floatSegment.incSize(length);
        }
        else {
            throwIncompatibleSegmentError(sourceSegment);
        }

        floatSegment.incSize(length);
    }


    // Support for resolution ------------------------------------------------------------------------------------------

    @Override
    public boolean supportsResolution() {
        return true;
    }

    /**
     * Is vector resolution automatically determined (by default true)
     *
     * @return true if the resolution was determined automatically
     */
    @Override
    public boolean isAutoResolution() {
        return super.isAutoResolution();
    }

    /**
     * Gets the vector's resolution
     *
     * @return
     */
    @Override
    public double getResolution() {
        return super.getResolution();
    }

    /**
     * sets the vector's resolution. Can only be done before it is sealed
     *
     * @param resolution
     */
    @Override
    public void setResolution(double resolution) throws DataSpaceException {
        super.setResolution(resolution);
    }

    /**
     * Based on the resolution, is there more than one distinct value in the vector?
     *
     * @return
     */
    @Override
    public boolean hasMoreThanOneValue() throws DataSpaceException {
        return super.hasMoreThanOneValue();
    }

    /**
     * Closes the vector and makes it immutable
     */
    @Override
    public FloatVector seal() throws DataSpaceException {
        super.seal();
        conditionallyCalculateResolution();
        return this;
    }

    @Override
    protected Numeric<Float> makeT(double restored) {
        return new Numeric<Float>((float )restored);
    }

    public DoubleSegmentStats getDescriptiveStats() throws DataSpaceException {
        requireSealedStatus(true);
        return (DoubleSegmentStats) getStats().getDescriptiveStats();
    } 

    /**
     * Produces a query result as a list of the base type of the vector
     * @param bitmap
     * @return
     */
    public List<Float> getMatchingBaseValues(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<Float> retval = new ArrayList<Float>();
        BitmapVectorIterator<Numeric<Float>> iterator = (BitmapVectorIterator<Numeric<Float>>) iterator(bitmap);
        while (iterator.hasNext()) {
            retval.add(iterator.next().getValue());
        }
        return retval;
    }

    public List<Number> asNumbers() throws DataSpaceException {
        ArrayList<Number> list = new ArrayList<Number>(size());
        for (Numeric<Float> num: asList()) {
            list.add(num.getValue());
        }
        return list;
    }

    @Override
    public DoubleSegmentStats getDescriptiveStats(IBitMap selection) throws DataSpaceException {
        if (selection==null) {
            return getDescriptiveStats();
        }
        // naive implementation
        DoubleSegmentStats stats = new DoubleSegmentStats(PersistenceType.TEMPORARY);
        BitmapVectorIterator<Numeric<Float>> iterator = (BitmapVectorIterator<Numeric<Float>>) iterator(selection);
        while (iterator.hasNext()) {
            stats.add(iterator.next().getDoubleValue());
        }
        return stats;
    }
}
