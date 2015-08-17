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

/**
 * Created: 12/9/10 5:02 PM
 * By: Arnon Moscona
 * Memory managed objects interact with a memory manager to decide when they should swap in and out of memory.
 * Note that the memory manager only tracks space use and decides which objects should be swapped in or out by calling
 * their appropriate methods. It does not know anything about persistence or how the swap operation is performed.
 * It is the responsibility of the object, working in concert with its data store to manage storage and to release
 * memory. usually this means that the managed object has a separate serializable payload that it knows how to store
 * and restore, and can nullify the reference to it when it is swapped out.
 */
public interface IMemoryManaged extends IStorable {
    public PersistenceStatus getPersistenceStatus();
    void setPersistenceStatus(PersistenceStatus status);
    public int getMemoryManagerId();
    public void setMemoryManagerId(int id);
    public void swapOut() throws DataSpaceException;

    public void swapIn() throws DataSpaceException;
}
