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

import com.moscona.util.monitoring.stats.LongSampleAccumulator;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.IMemoryManaged;
import com.moscona.dataSpace.persistence.IMemoryManager;
import com.moscona.dataSpace.persistence.PersistenceStatus;

import java.lang.ref.WeakReference;
import java.util.*;

/**
 * Created: 1/13/11 3:37 PM
 * By: Arnon Moscona
 */
public class SimpleMemoryManager implements IMemoryManager {
    private long maxBytes;
    private int maxId = 0;
    private HashMap<Integer, Record> records;
    private PriorityQueue<Record> lru;
    private HashSet<Integer> required;

    private int totalSwapInCounter = 0;
    private long totalSwapInBytes = 0;
    private int totalSwapOutCounter = 0;
    private long totalSwapOutBytes = 0;
    private long totalSwapInTime = 0L;
    private long totalUtilization = 0L;
    private long totalReleaseCounter=0L;
    private long totalRequireCounter=0L;
    private long highWaterMark = 0L;
    private int requireCounter = 0;
    private int submitCounter = 0;
    private int lastRequired = -1;
    private boolean lastRequiredPendingRelease = false;
    private LongSampleAccumulator requiredTimeStats; // accumulate stats for time objects spend being in required state

    public SimpleMemoryManager(long maxBytes) {
        this.maxBytes = maxBytes;
        records = new HashMap<Integer,Record>();
        lru = new PriorityQueue<Record>();
        required = new HashSet<Integer>();
        requiredTimeStats = new LongSampleAccumulator();
        //System.out.println("MM,op,record type,id,required size, lru size,swapIn counter,swapOut counter,utilization," +
        //        "util(MB),submit counter,submit+swapIn,require counter,release counter,require-release");
    }

    public long getMaxBytes() {
        return maxBytes;
    }

    public int getRequireCounter() {
        return requireCounter;
    }

    public int getSubmitCounter() {
        return submitCounter;
    }

    public long getTotalReleaseCounter() {
        return totalReleaseCounter;
    }

    public long getTotalRequireCounter() {
        return totalRequireCounter;
    }

    public long getTotalSwapInBytes() {
        return totalSwapInBytes;
    }

    public long getTotalSwapOutBytes() {
        return totalSwapOutBytes;
    }

    @Override
    public synchronized int submit(IMemoryManaged data) throws DataSpaceException {
        if (! shouldSubmit(data)) {
            return data.getMemoryManagerId();
        }

        Record rec = new Record(data);
        data.setMemoryManagerId(rec.id);
        if (data.getPersistenceStatus() == PersistenceStatus.NOT_PERSISTED) {
            swapIn(rec);
            rec.size = data.sizeInBytes(); // may need to be refreshed
        }
        records.put(rec.id,rec);
        if (data.getPersistenceStatus() == PersistenceStatus.SWAPPED_IN) {
            // if submitted as swapped out then do not add to LRU
            lruAppend(rec);
        }

        submitCounter++;
        return rec.id;
    }

    private void printStats(String op, Record rec) {
        String recType = rec.item.get()==null ? "null" : rec.item.get().getClass().getSimpleName();
//        System.out.println("MM," + op +"," + recType +"," + rec.id +"," + required.size() +"," + lru.size() +
//                "," + totalSwapInCounter + "," + totalSwapOutCounter +"," + totalUtilization +
//                "," + totalUtilization/1024/1024 +
//                "," + submitCounter +
//                "," + (submitCounter+totalSwapInCounter) +
//                "," + totalRequireCounter +
//                "," + totalReleaseCounter +
//                "," + (totalRequireCounter-totalReleaseCounter));
    }

    private boolean shouldSubmit(IMemoryManaged data) throws DataSpaceException {
        int id = data.getMemoryManagerId();
        if (id <0) {
            return true;
        }
        blowUpIfDoesNotExist(id, "Submitted");
        IMemoryManaged obj = records.get(id).item.get();
        if (obj!=data) {
            throw new DataSpaceException("Submitted an object to memory management ID that already has an ID, but the object is not the object originally registered with this ID");
        }
        return false;
    }

    private void blowUpIfDoesNotExist(int id, String operation) throws DataSpaceException {
        if (id>maxId || id<0) {
            throw new DataSpaceException(operation+" an object to memory management ID that already has an ID, which does not exist");
        }
    }

    private void swapIn(Record rec) throws DataSpaceException {
        IMemoryManaged item = rec.item.get();
        if (item==null) {
            throw new DataSpaceException("BUG! SimpleMemoryManager attempt to swap in a garbage collected item");
        }
        if (item.getPersistenceStatus()==PersistenceStatus.SWAPPED_IN) {
            return; // nothing to do...
        }

        long start = System.currentTimeMillis();
        try {
            item.swapIn();
        }
        finally {
            totalSwapInTime += (System.currentTimeMillis()-start);
        }
        rec.swapInCounter++;
        totalSwapInCounter++;
        totalSwapInBytes+=rec.size;
    }

    private void lruAppend(Record rec) throws DataSpaceException {
        if (eventualUtilization(rec.size) <= maxBytes) {
            // easiest case - just add it, don't worry about anything else
            simpleLruAdd(rec);
            return;
        }
        sweepForStaleOrSwappedOutReferences(); // find any references in the LRU that have been garbage collected
        if (eventualUtilization(rec.size) <= maxBytes) {
            simpleLruAdd(rec);
            return;
        }

        while(eventualUtilization(rec.size) > maxBytes && lru.size()>0) {
            Record tail = lru.poll();
            IMemoryManaged managed = tail.item.get();
            if (managed!=null && managed.getPersistenceStatus() == PersistenceStatus.SWAPPED_IN) {
                swapOut(tail, managed);
            }
            incrementUtilization(-tail.size);
        }
        simpleLruAdd(rec);
    }

    private long eventualUtilization(long increment) {
        return totalUtilization+increment;
    }

    private void sweepForStaleOrSwappedOutReferences() {
        ArrayList<Record> toRemove = new ArrayList<Record>();
        for (Record record: lru) {
            IMemoryManaged managed = record.item.get();
            if (managed == null || managed.getPersistenceStatus() == PersistenceStatus.SWAPPED_OUT) {
                toRemove.add(record); // avoid a concurrent modification exception
            }
        }
        for (Record record: toRemove) {
            lru.remove(record);
            incrementUtilization(-record.size);
            // no need to swap out as we know it's not swapped in even if it exists at all
        }
    }

    private void swapOut(Record record, IMemoryManaged managed) throws DataSpaceException {
        managed.swapOut();
        if (managed.getPersistenceStatus() != PersistenceStatus.SWAPPED_OUT) {
            throw new DataSpaceException("Bug! After swap out, object is in incorrect state: "+managed.getPersistenceStatus());
        }
        record.swapOutCounter++;
        totalSwapOutCounter++;
        totalSwapOutBytes+=record.size;
    }

    private void simpleLruAdd(Record rec) {
        touch(rec);
        lru.add(rec);
        incrementUtilization(rec.size);
    }

    private void touch(Record rec) {
        rec.lruTag = System.nanoTime();
    }

    private Record get(int id, String operation, boolean throwErrorOnGarbageObjects) throws DataSpaceException {
        blowUpIfDoesNotExist(id, operation);
        Record rec = records.get(id);
        if (rec==null) {
            throw new DataSpaceException("Bug! Could not find record for ID "+id+" even though I should have it");
        }
        if (throwErrorOnGarbageObjects && rec.item.get()==null) {
            throw new DataSpaceException("Error: stale record for ID "+id+" refers to an object that was already garbage collected");
        }
        return rec;
    }

    @Override
    public synchronized void require(int id) throws DataSpaceException {
        totalRequireCounter++;
        if (rapidFireRequire(id)) {
            return;
        }
        Record rec = get(id, "Required", true);
        IMemoryManaged obj = rec.item.get(); // keep a reference until we return to make sure the object does not go away
        long increment = rec.size;
        if (lru.contains(rec)) {
            lru.remove(rec);
            increment = 0L;
        }
        swapIn(rec); // will do nothing if already swapped in
        incrementUtilization(increment);
        addToRequired(id);
        touch(rec);
        requireCounter++;
        lastRequired = id;
        lastRequiredPendingRelease = false;
    }

    private void addToRequired(int id) {
        required.add(id);
        records.get(id).addedToRequiredTs = System.currentTimeMillis();
    }

    /**
     * Handles the case that this is the same ID that was required in the last call
     * @param id
     * @return
     */
    private boolean rapidFireRequire(int id) throws DataSpaceException {
        boolean retval = id == lastRequired;
        if (!retval && lastRequiredPendingRelease) {
            // we have a require on a new ID and the previous one is still pending
            normalRelease(lastRequired);
        }

        lastRequired = id;
        lastRequiredPendingRelease = false;
        return retval;
    }

    @Override
    public synchronized void release(int id) throws DataSpaceException {
        totalReleaseCounter++;
        if(rapidFireRelease(id)) {
            return;
        }
        normalRelease(id);
    }

    private void normalRelease(int id) throws DataSpaceException {
        Record rec = get(id, "Released", false);
        if (! required.contains(rec.id)) {
            throw new DataSpaceException("Attempt to release record ID "+id+" while it is not required...");
        }
        removeFromRequired(id);
        incrementUtilization(-rec.size);
        lruAppend(rec);
        lastRequired = -1;
        lastRequiredPendingRelease = false;
    }

    private synchronized void removeFromRequired(int id) {
        required.remove(id);
        Record record = records.get(id);
        if (record.addedToRequiredTs>0) {
            long span = System.currentTimeMillis() - record.addedToRequiredTs;
            requiredTimeStats.addSample(span);
        }
        record.addedToRequiredTs = -1L;
    }

    private boolean rapidFireRelease(int id) {
        if (id == lastRequired) {
            lastRequiredPendingRelease = true;
        }
        return lastRequiredPendingRelease;
    }

    @Override
    public synchronized void onSwappedOut(IMemoryManaged managed) throws DataSpaceException {
        Record rec = get(managed.getMemoryManagerId(), "Swap out notification", false);
        boolean removed = false;
        if (lru.contains(rec)) {
            removed = true;
            lru.remove(rec);
        }
        if (required.contains(rec.id)) {
            removed = true;
            removeFromRequired(rec.id);
        }
        if (removed) {
            incrementUtilization(-rec.size);
        }
        if (managed.getPersistenceStatus() != PersistenceStatus.SWAPPED_OUT) {
            managed.setPersistenceStatus(PersistenceStatus.SWAPPED_OUT);
        }
        if (lastRequired==rec.id) {
            lastRequired = -1;
            lastRequiredPendingRelease = false;
        }
    }

    @Override
    public long getMaxSize() {
        return maxBytes;
    }

    private long incrementUtilization(Long increment) {
        totalUtilization += increment;
        highWaterMark = Math.max(highWaterMark,totalUtilization);
        return totalUtilization;
    }

    public int getTotalSwapInCounter() {
        return totalSwapInCounter;
    }

    public long getTotalSwapInTime() {
        return totalSwapInTime;
    }

    public int getTotalSwapOutCounter() {
        return totalSwapOutCounter;
    }

    public long getTotalUtilization() {
        return totalUtilization;
    }

    public long getSlack() {
        return maxBytes-totalUtilization;
    }

    public long getHighWaterMark() {
        return highWaterMark;
    }

    /**
     * Calculates the amount of swap outs that are needed to satisfy the data use
     * @return swap outs / swap ins
     */
    public double getChurnRate() {
        if (totalSwapInCounter==0) {
            return 0.0;
        }
        return ((double)totalSwapOutCounter)/totalSwapInCounter;
    }

    /**
     * Calculates the amount of swap out bytes that are needed to satisfy the data use
     * @return swap out bytes / swap in bytes
     */
    public double getChurnRateByVolume() {
        if (totalSwapInBytes==0) {
            return 0.0;
        }
        return ((double)totalSwapOutBytes)/totalSwapInBytes;
    }

    /**
     * Calculates the average time spent on swapping data in per require request (does not include rapid-fire require
     * calls on the same object, as happens in external vector iteration)
     * @return
     */
    public double getRequireAvgIoCost() {
        if (requireCounter==0 || totalSwapInTime==0) {
            return 0.0;
        }
        return ((double)totalSwapInTime)/requireCounter;
    }

    public boolean isRequired(int id) {
        return required.contains(id);
    }

    public LongSampleAccumulator getRequiredTimeStats() {
        return requiredTimeStats;
    }

    /**
     * Checks all the currently required objects and finds all those that are outliers in the sense that they have been
     * required for at least (meanRequiredTime + stdevsOutOfMean*stdev(requiredTime)) msec and gives us information
     * about them.
     * @param stdevsOutOfMean - minimum number of standard deviations out of the mean (recommended: >=2)
     * @param absoluteLongMillis - absolute minimum number of milliseconds to be required old (recommended: >=50)
     * @param minSamples
     * @return
     */
    public synchronized List<MemoryManagedObjectInfo> getOldRequiredIds(double stdevsOutOfMean, long absoluteLongMillis, int minSamples) throws DataSpaceException {
        if (minSamples < 10) {
            throw new DataSpaceException("getOldRequiredIds() does not work with sample sets smaller than 10");
        }
        if (stdevsOutOfMean < 0.499) {
            throw new DataSpaceException("getOldRequiredIds() does not work with stdevsOutOfMean smaller than 0.5");
        }
        List<MemoryManagedObjectInfo> list = new ArrayList<MemoryManagedObjectInfo>();
        if (requiredTimeStats.count() < minSamples) {
            return list;
        }

        double threshold = Math.max(absoluteLongMillis, stdevsOutOfMean*requiredTimeStats.stdev()+requiredTimeStats.mean());
        for (int id: required) {
            Record rec = records.get(id);
            if (System.currentTimeMillis()-rec.addedToRequiredTs > threshold) {
                list.add(new MemoryManagedObjectInfo(rec));
            }
        }
        return list;
    }

    public List<MemoryManagedObjectInfo> getOldRequiredIds(double stdevsOutOfMean, long absoluteLongMillis) throws DataSpaceException {
        return getOldRequiredIds(stdevsOutOfMean, absoluteLongMillis, 100);
    }

    /**
     * A simple value class to keep weak references and associated stats
     */
    private class Record implements Comparable<Record> {
        public WeakReference<IMemoryManaged> item;
        public int id;
        public long size;
        public int swapInCounter;
        public int swapOutCounter;
        public long lruTag;
        public long addedToRequiredTs = -1L;
        public WeakReference<Thread> requiredInThreadReference;

        protected Record(IMemoryManaged item) throws DataSpaceException {
            this.item = new WeakReference<IMemoryManaged>(item);
            id = maxId++;
            size = item.sizeInBytes();
            swapInCounter=0;
            swapOutCounter=0;
            lruTag = -1L;
            requiredInThreadReference = new WeakReference<Thread>(Thread.currentThread());
        }

        @Override
        public int compareTo(Record o) {
            if (o.id == id) {
                return 0;
            }
            return (lruTag-o.lruTag) < 0 ? -1 : 1;
        }
    }

    public class MemoryManagedObjectInfo {
        private int id;
        private WeakReference<IMemoryManaged> objectReference;
        private WeakReference<Thread> requiredInThreadReference;
        private long requiredTimeStamp;
        private long timeRequiredSoFar;
        private int swapInCounter;
        private int swapOutCounter;
        private LongSampleAccumulator allStats=null;
        private StackTraceElement[] stackTraceAtSampleTime=null;

        protected MemoryManagedObjectInfo(Record record) {
            id = record.id;
            objectReference = record.item;
            requiredInThreadReference = record.requiredInThreadReference;
            requiredTimeStamp = record.addedToRequiredTs;
            timeRequiredSoFar = System.currentTimeMillis() - requiredTimeStamp;
            swapInCounter = record.swapInCounter;
            swapOutCounter = record.swapOutCounter;
            try {
                allStats = requiredTimeStats.clone();
            }
            catch (CloneNotSupportedException e) {
                // ignore, leave null
            }
            Thread requiringThread = record.requiredInThreadReference.get();
            if (requiringThread!=null) {
                stackTraceAtSampleTime = requiringThread.getStackTrace();
            }
        }

        public int getId() {
            return id;
        }

        public WeakReference<IMemoryManaged> getObjectReference() {
            return objectReference;
        }

        public WeakReference<Thread> getRequiredInThreadReference() {
            return requiredInThreadReference;
        }

        public long getRequiredTimeStamp() {
            return requiredTimeStamp;
        }

        public int getSwapInCounter() {
            return swapInCounter;
        }

        public int getSwapOutCounter() {
            return swapOutCounter;
        }

        public LongSampleAccumulator getAllStats() {
            return allStats;
        }

        public long getTimeRequiredSoFar() {
            return timeRequiredSoFar;
        }
    }
}
