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

package com.moscona.dataSpace;

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.persistence.IMemoryManaged;
import com.moscona.dataSpace.persistence.PersistenceStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Created: Dec 8, 2010 2:22:38 PM
 * By: Arnon Moscona
 * The base interface for concrete implementations of vector segments.
 * Vector segments are memory managed and they use the backing array as the actual payload that gets swapped in and out,
 * while the segment itself (sans the data) remains in memory, including its descriptive stats.
 */
public interface IVectorSegment<ScalarType extends IScalar> extends IMemoryManaged {
//    public IVectorSegment<ScalarType> append(ScalarType element);
    public void seal();
    public boolean isSealed();

    /**
     * Used by segment queries that are not deeply integrated with the native segment class. Iteration increases
     * query time for range queries (over direct access to the backing array) by an estimated multiplier of 4-5
     * @return
     */
    public ISegmentIterator<ScalarType> iterator();

    @Override
    public long sizeInBytes();

    public ISegmentStats getSegmentStats();

    public void setSegmentNumber(int segmentNo);

    public ISegmentStats calculateStats();

    /**
     * Requires the backing array for the segment
     */
    public void require() throws DataSpaceException;

    /**
     * Releases the backing array for the segment
     */
    public void release() throws DataSpaceException;

    public void release(boolean quiet) throws DataSpaceException;

    public int getSegmentNumber();

    @Override
    public void setPersistenceStatus(PersistenceStatus persistenceStatus);

    /**
     * The actual number of elements in the segment, regardless of allocated space
     * @return
     */
    public int size();

    public Set<ScalarType> getUniqueValues();

    /**
     * Required in vector segments of real numbers (float, double)
     * @param resolution
     * @return
     */
    public Set<? extends ScalarType> getUniqueValues(double resolution);

    public double[] copyAsDoubles();

    public void estimateQuantilesOnRestOfSegments(Quantiles quantiles);

    void setVector(IVector<ScalarType> vector);

    boolean isBackingArrayLoaded();

    /**
     * Appends selection values to a return value
     * @param retval the list to append values to
     * @param positions the positions of interest (vector indices) - if null append all
     * @param from - hint first index of interest (if positions  not null)
     * @param to - hint last index of interest (if positions  not null)
     */
    void appendValues(ArrayList<ScalarType> retval, List<Integer> positions, Integer from, Integer to);
}
