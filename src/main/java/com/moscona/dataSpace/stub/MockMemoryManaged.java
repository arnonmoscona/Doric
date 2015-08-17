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
import com.moscona.dataSpace.persistence.PersistenceType;

/**
 * Created: 1/13/11 3:27 PM
 * By: Arnon Moscona
 */
public class MockMemoryManaged implements IMemoryManaged {
    private static final long serialVersionUID = -793389389546942411L; // not that it really matters here
    private PersistenceType persistenceType = PersistenceType.TEMPORARY;
    private PersistenceStatus persistenceStatus = PersistenceStatus.NOT_PERSISTED;
    private int memoryManagerId = -1;
    private long sizeInBytes = 0L;
    private IMemoryManager memoryManager;
    private int swapInCounter = 0;
    private int swapOutCounter = 0;

    public MockMemoryManaged(long sizeInBytes, IMemoryManager manager) {
        this.sizeInBytes = sizeInBytes;
        memoryManager = manager;
    }

    public IMemoryManager getMemoryManager() {
        return memoryManager;
    }

    public void setMemoryManager(IMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public int getMemoryManagerId() {
        return memoryManagerId;
    }

    @Override
    public void setMemoryManagerId(int memoryManagerId) {
        this.memoryManagerId = memoryManagerId;
    }

    @Override
    public synchronized void swapOut() throws DataSpaceException {
        swapInCounter++;
        persistenceStatus = PersistenceStatus.SWAPPED_OUT;
        memoryManager.onSwappedOut(this);
    }

    @Override
    public synchronized void swapIn() throws DataSpaceException {
        swapOutCounter++;
        persistenceStatus = PersistenceStatus.SWAPPED_IN;
        try {
            Thread.sleep(10);
        }
        catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public PersistenceStatus getPersistenceStatus() {
        return persistenceStatus;
    }

    @Override
    public void setPersistenceStatus(PersistenceStatus persistenceStatus) {
        this.persistenceStatus = persistenceStatus;
    }

    @Override
    public PersistenceType getPersistenceType() {
        return persistenceType;
    }

    @Override
    public void setPersistenceType(PersistenceType persistenceType) {
        this.persistenceType = persistenceType;
    }

    @Override
    public long sizeInBytes() throws DataSpaceException {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public int getSwapInCounter() {
        return swapInCounter;
    }

    public void setSwapInCounter(int swapInCounter) {
        this.swapInCounter = swapInCounter;
    }

    public int getSwapOutCounter() {
        return swapOutCounter;
    }

    public void setSwapOutCounter(int swapOutCounter) {
        this.swapOutCounter = swapOutCounter;
    }
}
