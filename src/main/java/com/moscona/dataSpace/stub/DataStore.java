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

package com.moscona.dataSpace.stub;

import com.moscona.exceptions.InvalidStateException;
import com.moscona.dataSpace.DataSpace;
import com.moscona.dataSpace.IVector;
import com.moscona.dataSpace.IVectorSegment;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.IDataStore;

/**
 * Created: 12/13/10 7:22 PM
 * By: Arnon Moscona
 */
public class DataStore implements IDataStore {
    Integer segmentSize = null;

    /**
     * Interrogates the data store as to whether it has any data in it. If it does - then its segment size is "baked in"
     *
     * @return
     */
    @Override
    public boolean isEmpty() {
        return true;
    }

    /**
     * Interrogates the data store as to its segment size. Empty data stores should return null.
     *
     * @return
     */
    @Override
    public Integer getSegmentSize() {
        return segmentSize;
    }

    /**
     * Sets the segment size for the data store. This is only allowed if the data store is empty.
     *
     * @param size
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *          if the segment size is already "baked in" and may not be changed
     */
    @Override
    public void setSegmentSize(int size) throws DataSpaceException {
        segmentSize = size;
    }

    @Override
    public void dumpSegment(IVectorSegment segment) throws DataSpaceException {
        // do nothing
    }

    @Override
    public void restoreSegment(IVectorSegment segment) throws DataSpaceException, InvalidStateException {
        // do nothing
    }

    @Override
    public void moveAllSegments(IVector vector, boolean fromTemporary, boolean toTemporary) {
        // do nothing
    }

    @Override
    public void dumpAllSegments(IVector vector) {
        // do nothing
    }

    @Override
    public void dump(DataSpace dataSpace) throws DataSpaceException {
        // do nothing
    }

    @Override
    public void deleteSummary(DataSpace dataSpace) {
        // do nothing
    }

    @Override
    public void register(DataSpace dataSpace) {
        // do nothing
    }

    @Override
    public void register(IVector vector) {
        // do nothing
    }

    @Override
    public void close() throws DataSpaceException {
        // do nothing
    }

    @Override
    public void dumpDataSpaceSummary(DataSpace dataSpace) throws DataSpaceException {
        // do nothing
    }
}
