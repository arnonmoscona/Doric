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

package com.moscona.dataSpace.impl.segment;

import com.moscona.exceptions.InvalidStateException;
import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.PersistenceStatus;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.io.File;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created: 12/9/10 7:37 PM
 * By: Arnon Moscona
 * A base class for concrete vector segments. Vector segments are increasingly C-like structures as you go down the
 * class hierarchy and are designed for maximizing query throughput.
 */
public abstract class AbstractVectorSegment<ScalarType extends IScalar,NativeType> implements IVectorSegment<ScalarType> {
    private static final long serialVersionUID = 4197485102230422006L;
    private transient IVectorSegmentBackingArray<NativeType> backingArray; // IMPORTANT the backing array is TRANSIENT and that the segment only knows its segment index within the vector and can get the actual segment from the data store
    private DataSpace dataSpace;
    private transient PersistenceStatus persistenceStatus;
    private boolean isRequired; // IMPORTANT: DO NOT USE - TO BE DELETED not transient. boolean will default to false, which is correct, when first loaded
    private transient AtomicInteger requireCounter = new AtomicInteger(0); // in a concurrent GUI environment the boolean flag is insufficient
    private int size;
    private int maxSegmentSize;

    protected ISegmentStats stats;
    private int memoryManagerId;
    private boolean isSealed = false;
    private PersistenceType persistenceType;
    private int segmentNumber=-1;
    private IVector<ScalarType> vector=null;
    private Integer dataStoreSegmentId = null;
    private String backingArrayStorageLocation = null;

    protected AbstractVectorSegment(DataSpace dataSpace, PersistenceType persistenceType, int maxSegmentSize, AbstractSegmentStats segmentStats) {
        this.dataSpace = dataSpace;
        this.persistenceType = persistenceType;
        this.maxSegmentSize = maxSegmentSize;
        stats = segmentStats;
        persistenceStatus = PersistenceStatus.NOT_PERSISTED;
        memoryManagerId = -1;
        backingArray = null;
        isRequired = false;
        isSealed = false;
        size = 0;
    }

    protected abstract void trimBackingArray();
    protected abstract IVectorSegmentBackingArray<NativeType> createBackingArray();

    private boolean isRequired() {
        synchronized (dataSpace.getMemoryManager()) {
            return requireCounter.get()>0;
        }
    }

    private void setRequired(boolean isRequired) throws DataSpaceException {
//        this.isRequired = isRequired;
        synchronized (dataSpace.getMemoryManager()) {
            int counter = requireCounter.addAndGet(isRequired ? 1 : -1);
            this.isRequired = (counter > 0);
            if (counter < 0) {
                throw new DataSpaceException("Concurrency bug: require counter fell below zero: "+requireCounter);
            }
        }
    }

    private String whichVector() {
        return vector.toString();
    }

    @Override
    public void seal() {
        if (isSealed()) {
            return;
        }
        trimBackingArray();
    }

    @Override
    public boolean isSealed() {
        return isSealed;
    }

    @Override
    public PersistenceType getPersistenceType() {
        return persistenceType;
    }

    /**
     * Changes the persistence type for the value.
     *
     * @param persistenceType
     */
    @Override
    public void setPersistenceType(PersistenceType persistenceType) {
        this.persistenceType = persistenceType;
    }

    @Override
    public ISegmentStats getSegmentStats() {
        return stats;
    }

    @Override
    public void setSegmentNumber(int segmentNo) {
        segmentNumber = segmentNo;
    }

    @Override
    public int getSegmentNumber() {
        return segmentNumber;
    }

    /**
     * Requires the backing array for the segment
     */
    @Override
    public void require() throws DataSpaceException {
        synchronized (dataSpace.getMemoryManager()) {
            //noinspection SynchronizeOnNonFinalField
            synchronized (requireCounter) {
                if (isRequired()) {
                    requireCounter.incrementAndGet();
                    return;
                }
                if (memoryManagerId < 0) {
                    throw new DataSpaceException("Vector segment required but never submitted (no memory manager ID)");
                }
                if (backingArray==null && persistenceStatus!=PersistenceStatus.SWAPPED_OUT) {
                    throw new DataSpaceException("required a segment without a backing array present and not swapped out");
                }
                if (requireCounter.get()==0) {
                    // will increment to 1 - memory manager has no counters
                    dataSpace.getMemoryManager().require(memoryManagerId);
                }

                setRequired(true);
            }
        }
    }

    /**
     * Releases the backing array for the segment
     */
    @Override
    public void release(boolean quiet) throws DataSpaceException {
        synchronized (dataSpace.getMemoryManager()) {
            //noinspection SynchronizeOnNonFinalField
            synchronized (requireCounter) {
                if (!isRequired()) {
                    if (!quiet) {
                        throw new DataSpaceException("Release called on a segment that's not required. Segment "+segmentNumber+" of "+whichVector());
                    }
                    return;
                }
                if (requireCounter.get()==1) {
                    // will drop to zero - memory manager has no counters
                    dataSpace.getMemoryManager().release(memoryManagerId);
                }
                setRequired(false);
            }
        }
    }

    private String getDebugId() {
        int segmentNumber = getSegmentNumber();
        String baseType = getVector().getBaseType().toString();
        return "segment #"+segmentNumber+" of "+baseType+" vector "+getVector();
    }

    @Override
    public void release() throws DataSpaceException {
        release(false);
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
    public int getMemoryManagerId() {
        return memoryManagerId;
    }

    @Override
    public void setMemoryManagerId(int id) {
        memoryManagerId = id;
    }

    public IVectorSegmentBackingArray<NativeType> getBackingArray() {
        if (backingArray==null) {
            backingArray = createBackingArray();
        }
        return backingArray;
    }


    public void setBackingArray(IVectorSegmentBackingArray<NativeType> backingArray) {
        this.backingArray = backingArray;
    }

    public DataSpace getDataSpace() {
        return dataSpace;
    }

    public ISegmentStats getStats() {
        return stats;
    }

    public void incSize(int length) throws DataSpaceException {
        size+=length;
        if (size>maxSegmentSize) {
            throw new DataSpaceException("Segment size incremented beyond allowed size: incremented by "+length+" to "+size+" > "+maxSegmentSize);
        }
    }

    /**
     * The actual number of elements in the segment, regardless of allocated space
     *
     * @return
     */
    @Override
    public int size() {
        return size;
    }

    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }

    @Override
    public Set<? extends ScalarType> getUniqueValues(double resolution) {
        return getUniqueValues(); // by default just ignore the resolution
    }

    @Override
    public void setVector(IVector<ScalarType> vector) {
        this.vector = vector;
    }

    public IVector<ScalarType> getVector() {
        return vector;
    }

    @Override
    public boolean isBackingArrayLoaded() {
        return backingArray!=null;
    }

    public Integer getDataStoreSegmentId() {
        return dataStoreSegmentId;
    }

    public void setDataStoreSegmentId(int dataStoreSegmentId) {
        this.dataStoreSegmentId = dataStoreSegmentId;
    }

    public void setBackingArrayStorageLocation(String backingArrayStorageLocation) {
        this.backingArrayStorageLocation = backingArrayStorageLocation;
    }

    public String getBackingArrayStorageLocation() {
        return backingArrayStorageLocation;
    }

    @Override
    public void swapIn() throws DataSpaceException {
        synchronized (dataSpace.getMemoryManager()) {
            try {
                vector.getDataSpace().getDataStore().restoreSegment(this);
            }
            catch (InvalidStateException e) {
                throw new DataSpaceException("Exception while restoring backing array: "+e,e);
            }
        }
    }

    @Override
    public void swapOut() throws DataSpaceException {
        synchronized (dataSpace.getMemoryManager()) {
            swapOutUnchecked();
            vector.getDataSpace().getMemoryManager().onSwappedOut(this);
        }
    }

    public void swapOutUnchecked() {
        synchronized (dataSpace.getMemoryManager()) {
            backingArray = null; // can be garbage collected
            persistenceStatus = PersistenceStatus.SWAPPED_OUT;
        }
    }

    public void initAfterLoadFromDisk() throws DataSpaceException {

        synchronized (dataSpace.getMemoryManager()) {
            requireCounter = new AtomicInteger(0);
            memoryManagerId = -1; // we don't really have one
            swapOutUnchecked();
            memoryManagerId = dataSpace.getMemoryManager().submit(this); // in a checked out state - we just need a new ID
//        requireCounter = 0;
        }
    }

    public int startingIndex() {
        return segmentNumber*maxSegmentSize;
    }
}
