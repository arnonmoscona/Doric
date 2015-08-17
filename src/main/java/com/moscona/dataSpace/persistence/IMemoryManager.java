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

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.segment.AbstractVectorSegment;

/**
 * Created: 12/9/10 6:25 PM
 * By: Arnon Moscona
 * An interface for classes that manage memory usage and virtual memory (for IMemoryManaged data items)
 */
public interface IMemoryManager {
    /**
     * Submits a piece of data to be memory managed. Note that the piece may already have been submitted to the memory
     * manager and the memory manager must check to not have duplicate entries. Specifically, a piece may be resubmitted
     * as part of its swap-in operation...
     * also, the piece may be submitted in a swapped out state, where its is not actually consuming memory, but does
     * need to get a memory manager ID
     * @param data
     * @return the memory manager ID of this newly submitted piece of data
     */
    public int submit(IMemoryManaged data) throws DataSpaceException;

    /**
     * requires this object for use and taking it out of memory management until it is released
     * @param id
     */
    public void require(int id) throws DataSpaceException;

    /**
     * releases this object back to the memory manager, allowing it to be swapped out if needed and making it as having
     * been used at this time (e.g. by placing it in an LRU list)
     * @param id
     */
    public void release(int id) throws DataSpaceException;

    /**
     * informs the memory manager that a swap out operation was completed
     * @param managed
     */
    void onSwappedOut(IMemoryManaged managed) throws DataSpaceException;

    long getMaxSize();
}
