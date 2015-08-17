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

import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.IMemoryManaged;
import com.moscona.dataSpace.persistence.IMemoryManager;
import com.moscona.dataSpace.persistence.PersistenceStatus;

import java.util.HashMap;

/**
 * Created: 12/13/10 7:20 PM
 * By: Arnon Moscona
 */
public class MemoryManager implements IMemoryManager {
    private int counter ;
    private HashMap<Integer, IMemoryManaged> managedObjects; // fake it's not really managed

    public MemoryManager() {
        counter = 0;
        managedObjects = new HashMap<Integer,IMemoryManaged>();
    }
    /**
     * Submits a piece of data to be memory managed
     *
     * @param data
     * @return the memory manager ID of this newly submitted piece of data
     */
    @Override
    public int submit(IMemoryManaged data) {
        int id = ++counter;
        managedObjects.put(id,data);
        return id;
    }

    /**
     * requires this object for use and taking it out of memory management until it is released
     *
     * @param id
     */
    @Override
    public void require(int id) throws DataSpaceException {
        IMemoryManaged data = managedObjects.get(id);
        if (data!=null && data.getPersistenceStatus()== PersistenceStatus.SWAPPED_OUT) {
            data.swapIn();
        }
    }

    /**
     * releases this object back to the memory manager, allowing it to be swapped out if needed and making it as having
     * been used at this time (e.g. by placing it in an LRU list)
     *
     * @param id
     */
    @Override
    public void release(int id) {
        // do nothing
    }

    @Override
    public void onSwappedOut(IMemoryManaged managed) {
        managed.setPersistenceStatus(PersistenceStatus.SWAPPED_OUT);
        // do nothing
    }

    @Override
    public long getMaxSize() {
        return 0;  // irrelevant here
    }
}
