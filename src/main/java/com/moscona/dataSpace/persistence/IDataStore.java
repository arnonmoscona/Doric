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

package com.moscona.dataSpace.persistence;

import com.moscona.exceptions.InvalidStateException;
import com.moscona.dataSpace.DataSpace;
import com.moscona.dataSpace.IVector;
import com.moscona.dataSpace.IVectorSegment;
import com.moscona.dataSpace.exceptions.DataSpaceException;

import java.io.FileNotFoundException;

/**
 * Created: 12/9/10 6:27 PM
 * By: Arnon Moscona
 */
public interface IDataStore {
    /**
     * Interrogates the data store as to whether it has any data in it. If it does - then its segment size is "baked in"
     * @return
     */
    public boolean isEmpty();

    /**
     * Interrogates the data store as to its segment size. Empty data stores should return null.
     * @return
     */
    public Integer getSegmentSize();

    /**
     * Sets the segment size for the data store. This is only allowed if the data store is empty.
     * @param size
     * @throws DataSpaceException if the segment size is already "baked in" and may not be changed
     */
    public void setSegmentSize(int size) throws DataSpaceException;

    void dumpSegment(IVectorSegment segment) throws DataSpaceException;

    @SuppressWarnings({"unchecked"}) // setting the backing array without checking type compatibility
    void restoreSegment(IVectorSegment segment) throws DataSpaceException, InvalidStateException;

    /**
     * Changes all segments of a vector from one temporary state to another
     * @param vector
     * @param fromTemporary
     * @param toTemporary
     */
    void moveAllSegments(IVector vector, boolean fromTemporary, boolean toTemporary) throws DataSpaceException;

    /**
     * makes sure all segments of a vector are stored. This is used when a vector is moved from a memory only state
     * (nothing stored) to a persistent or temporary state (stored)
     * @param vector
     */
    void dumpAllSegments(IVector vector) throws DataSpaceException;

    /**
     * Saves the data space to the data store
     * @param dataSpace
     */
    void dump(DataSpace dataSpace) throws DataSpaceException;
    void deleteSummary(DataSpace dataSpace);

    public void dumpDataSpaceSummary(DataSpace dataSpace) throws DataSpaceException, FileNotFoundException;
    void register(DataSpace dataSpace);

    void register(IVector vector);

    void close() throws DataSpaceException;
}
