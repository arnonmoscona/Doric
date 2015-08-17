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
import com.moscona.dataSpace.impl.segment.StringSegment;
import com.moscona.dataSpace.impl.segment.StringSegmentBackingArray;
import com.moscona.dataSpace.impl.segment.StringSegmentStats;
import com.moscona.exceptions.NotImplementedException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created: 12/10/10 2:26 PM
 * By: Arnon Moscona
 */
public class StringVector extends AbstractVector<Text> {
    private static final long serialVersionUID = -3452523978491194815L;
    StringSegment lastCreatedSegment;

    public StringVector(DataSpace dataSpace) {
        super(dataSpace);
        lastCreatedSegment = null;
    }

    public StringVector(String[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for (String value: values) {
            append(value, true);
        }
    }

    public StringVector(Text[] values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        append(values);
    }

    public StringVector(List<Text> values, DataSpace dataSpace) throws DataSpaceException {
        this(dataSpace);
        // the following is not optimized. Keeping is simple for now. Vector construction is not the key optimization here
        for(Text value: values) {
            append(value);
        }
    }

    /**
     * Creates a new vector in the same data space
     *
     * @return
     */
    @Override
    protected AbstractVector<Text> createNew() {
        return new StringVector(getDataSpace());
    }

    /**
     * Allows us to explicitly identify the base type (of the scalars in the vector)
     *
     * @return
     */
    @Override
    public BaseType getBaseType() {
        return BaseType.STRING;
    }

    public final void append(String[] values) throws DataSpaceException {
        for(String value: values) {
            append(value, true);
        }
    }

    public final void append(String value, int numberOfTimes) throws DataSpaceException {
        for (int i=0; i<numberOfTimes; i++) {
            append(value);
        }
    }

    /**
     * Appends the value and increments the vector size
     * @param value
     * @throws DataSpaceException
     */
    public void append(String value) throws DataSpaceException {
        append(value, true);
    }

    /**
     * Appends the value and optionally increments the vector size
     * @param value
     * @param incrementSize
     * @throws DataSpaceException
     */
    protected final void append(String value, boolean incrementSize) throws DataSpaceException {
        StringSegment segment = (StringSegment) getSegmentForIndex(size(),true);
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
    protected IVectorSegment<Text> createNewSegment() {
        lastCreatedSegment = new StringSegment(getDataSpace(), getPersistenceType(), getSegmentSize());
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
    protected void abstractAppend(Text element) throws DataSpaceException {
        StringSegment segment = (StringSegment) getSegmentForIndex(size(),true);
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
    protected Text get(IVectorSegment segment, int segmentIndex) {
        int code = ((StringSegmentBackingArray)((StringSegment)segment).getBackingArray()).data[segmentIndex];
        String value = getDataSpace().decodeToString(code);
        return new Text(value);
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
        int[] src = ((StringSegmentBackingArray)((StringSegment)sourceSegment).getBackingArray()).data;  // IMPORTANT: do not wrap in a method so as not to copy the array
        StringSegment stringSegment = (StringSegment) workingSegment;
        int[] dst = ((StringSegmentBackingArray) stringSegment.getBackingArray()).data;
        System.arraycopy(src,pos,dst,start,length);
        stringSegment.incSize(length);
    }

    public StringSegmentStats getDescriptiveStats() throws DataSpaceException {
        requireSealedStatus(true);
        return (StringSegmentStats) getStats().getDescriptiveStats();
    }   

    /**
     * Produces a query result as a list of the base type of the vector
     * @param bitmap
     * @return
     */
    public List<String> getMatchingBaseValues(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<String> retval = new ArrayList<String>();
        BitmapVectorIterator<Text> iterator = (BitmapVectorIterator<Text>) iterator(bitmap);
        while (iterator.hasNext()) {
            retval.add(iterator.next().getValue());
        }
        return retval;
    }

    @Override
    public IDescriptiveStats<?> getDescriptiveStats(IBitMap selection) throws DataSpaceException, NotImplementedException {
        throw new NotImplementedException("Descriptive stats not meaningful in this context");
    }
}
