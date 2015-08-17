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
import com.moscona.dataSpace.impl.StringVector;
import com.moscona.dataSpace.impl.segment.AbstractVectorSegment;
import com.moscona.dataSpace.persistence.DirectoryDataStore;
import com.moscona.dataSpace.persistence.IDataStore;
import com.moscona.dataSpace.persistence.IMemoryManager;
import com.moscona.dataSpace.persistence.PersistenceType;
import org.apache.commons.lang3.StringUtils;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Created: Dec 8, 2010 2:13:56 PM
 * By: Arnon Moscona
 * The root of a data set associated with a persistent store
 */
public class DataSpace implements INameSpace, Serializable {
    private static final long serialVersionUID = -2786506672012693871L; // changed serialVersionUID as do not want old data spaces to deserialize
    /**
     * The default preferred segment size (this is not guaranteed as it must be negotiated with the data store)
     */
    public static final int DEFAULT_PREFERRED_SEGMENT_SIZE = 1024*1024;
    public static final PersistenceType DEFAULT_PERSISTENCE_TYPE = PersistenceType.TEMPORARY;
    public static final int DEFAULT_THRESHOLD_FOR_PRECISE_QUANTILES = DEFAULT_PREFERRED_SEGMENT_SIZE;
    public static final int DEFAULT_RESOLUTION_RANGE_DIVISOR = 10000;

    private transient IMemoryManager memoryManager;
    private transient IDataStore dataStore;
    private PersistenceType defaultPersistenceType; // determines which name space would be the default one
    private int preferredSegmentSize;
    /**
     * The current implementation uses a fixed segment size for the entire data space and associated data store.
     * The segment size is negotiated with the data store. If there are already objects in the data store then its
     * segment size determines the segment size. Otherwise the user requested one (or the default) is used.
     *
     * This makes for simpler code as once set - the segment size is always known ahead of time and is the same for all
     * segments of all vectors.
     *
     * Another possible advantage of this is that if we ever want to evaluate a data frame row by row (as RDBMSs do)
     * then having the same segment size in all columns makes it easier to manage segment loading (should we want to do
     * the crazy thing of directly controlling this)
     *
     * The major down side of this approach is that the segments are not physically the same size. This can cause
     * problems in two areas. First - it can cause the heap to fragment more badly then it would has backing arrays
     * been exactly the same size iny bytes (interchangeable memory regions during collection or allocation) - this
     * is not sufficient to complicate the code for fixed byte size segments unless this fragmentation proves to be
     * a problem. Most JVMs have compacting garbage collectors that will eventually compact the heap anyway. So its
     * not clear how much performance degradation this choice may have until sufficient experience is gathered.
     *
     * The second area where the byte size of backing arrays may cause problems is if we choose to make a memory manager
     * that takes a more active roll. The first implementation only tracks memory utilization and instructs segments
     * to swap out their backing arrays or to swap them in. This leaves the actual memory block management to the JVM.
     * If the JVM ends up having a hard time balancing the large blocks of memory used by the backing arrays with the
     * tiny blocks that plain vanilla objects use - then it could make sense to "take over" direct management of these
     * block by pre-allocating resources and holding references to them in the memory manager - never letting them
     * get garbage collected. If this were C++ then you could manage your own heap for those and provision data of
     * arbitrary type out of it. It would then behoove you to want all backing arrays to be the same byte size, which
     * makes the private heap very simple (does not ever need to be compacted).
     *
     * Java seems to have no mechanism to allocate a ByteBuffer and use it as a byte[] some of the time and as an int[]
     * or some other array another time. You also cannot trivially point into a middle of an array and get an array
     * reference at that address. So at minimum an active memory manager would need to maintain at least one array per
     * primitive type (a "heap" for compatible backing arrays). In this scenario the byte size of backing arrays of
     * different base types does not matter. As Java seems to provide no way to "morph" primitive array types overlaid
     * on the same physical memory region (as you can at your own risk in C++), the point is moot.
     */
    private int segmentSize;
    private int defaultResolutionRangeDivisor = DEFAULT_RESOLUTION_RANGE_DIVISOR;

    private transient DataBundle defaultNameSpace=null;
    private DataBundle persistentNameSpace=null;
    private transient DataBundle tempNameSpace;
    private transient DataBundle ramNameSpace;
    private transient HashMap<String,DataBundle> dataBundles;

    private int preciseQuantileCalculationThreshold = DEFAULT_THRESHOLD_FOR_PRECISE_QUANTILES;
    private String name;
    private transient AtomicInteger changesInProgress = new AtomicInteger(0);
    private transient AtomicLong lastFlush = new AtomicLong(System.currentTimeMillis());
    private transient CloseHelper closeHelper;
    private HashMap<String,Integer> stringEncoding;
    private HashMap<Integer,String> stringDecoding;

    public DataSpace(IDataStore dataStore, IMemoryManager memoryManager, PersistenceType defaultPersistenceType, int preferredSegmentSize) throws DataSpaceException {
        this.dataStore = dataStore;
        this.memoryManager = memoryManager;
        this.defaultPersistenceType = defaultPersistenceType;
        this.preferredSegmentSize = preferredSegmentSize;
        stringDecoding = new HashMap<Integer,String>();
        stringEncoding = new HashMap<String,Integer>();

        negotiateSegmentSize();

        name = makeRandomName();
        initNameSpaces(defaultPersistenceType);
        closeHelper = new CloseHelper();

        //noinspection ThisEscapedInObjectConstruction
        dataStore.register(this);
    }

    /**
     * This method is called by the data store after the object was loaded from disk and before it returns so that the
     * data space can recover to a functional state with all transients in a reasonable shape.
     * DO NOT CALL unless you're part of the implementation (Java has no friends)
     * @param dataStore
     * @param memoryManager
     */
    public void initTransientsAfterRestore(IDataStore dataStore, IMemoryManager memoryManager) throws DataSpaceException {
        closeHelper = new CloseHelper();
        this.memoryManager = memoryManager;
        this.dataStore = dataStore;
        initNameSpaces(defaultPersistenceType);
        changesInProgress = new AtomicInteger(0);
        lastFlush = new AtomicLong(System.currentTimeMillis());
        // now we need to find all the vectors in the persistent data space and iterate over their segments and mark
        // them all as swapped out
        for (String name: persistentNameSpace.keySet()) {
            IDataElement element = persistentNameSpace.get(name);
            element.setNameSpace(persistentNameSpace);
            element.setPersistenceType(PersistenceType.PERSISTENT);
            if (AbstractVector.class.isAssignableFrom(element.getClass())) {
                AbstractVector vector = (AbstractVector) element;
                vector.initCloseHelper();
                vector.setDataSpace(this);
                vector.markAllSegmentsSwappedOut();
            }
        }
    }


    private String makeRandomName() {
        return "DataSpace_"+Double.toString(Math.random()).replaceAll("\\.","");
    }

    private void initNameSpaces(PersistenceType defaultPersistenceType) {
        if (dataBundles==null) {
            dataBundles = new HashMap<String,DataBundle>();
        }

        tempNameSpace = new DataBundle(this, PersistenceType.TEMPORARY, "temp");
        tempNameSpace.setTemporary(true);
        dataBundles.put("__temp", tempNameSpace);
        if (persistentNameSpace==null) {
            persistentNameSpace = new DataBundle(this, PersistenceType.PERSISTENT, "persistent");
            persistentNameSpace.setPersistent(true);
        }
        dataBundles.put("__persistent", persistentNameSpace);
        ramNameSpace = new DataBundle(this, PersistenceType.MEMORY_ONLY, "memory");
        dataBundles.put("__memory", ramNameSpace);

        switch(defaultPersistenceType) {
            case MEMORY_ONLY:
                defaultNameSpace = ramNameSpace;
                break;
            case TEMPORARY:
                defaultNameSpace = tempNameSpace;
                break;
            case PERSISTENT:
                defaultNameSpace = persistentNameSpace;
                break;
        }
    }

    private void negotiateSegmentSize() throws DataSpaceException {
        if (dataStore==null) {
            segmentSize = preferredSegmentSize;
        }
        else {
            if (dataStore.isEmpty()) {
                segmentSize = preferredSegmentSize;
                dataStore.setSegmentSize(preferredSegmentSize);
            }
            else {
                segmentSize = dataStore.getSegmentSize();
            }
        }

        if (segmentSize <= 0) {
            throw new DataSpaceException("After negotiation with data store ended up with a bad segment size of "+segmentSize);
        }
    }

    public DataSpace(IDataStore dataStore, IMemoryManager memoryManager, PersistenceType defaultPersistenceType) throws DataSpaceException {
        this(dataStore, memoryManager, defaultPersistenceType, DEFAULT_PREFERRED_SEGMENT_SIZE);
    }

    public DataSpace(IDataStore dataStore, IMemoryManager memoryManager) throws DataSpaceException {
        this(dataStore, memoryManager, DEFAULT_PERSISTENCE_TYPE, DEFAULT_PREFERRED_SEGMENT_SIZE);
    }

    public int getPreciseQuantileCalculationThreshold() {
        return preciseQuantileCalculationThreshold;
    }

    public void setPreciseQuantileCalculationThreshold(int preciseQuantileCalculationThreshold) {
        this.preciseQuantileCalculationThreshold = preciseQuantileCalculationThreshold;
    }

    public INameSpace getDefaultNameSpace() throws DataSpaceException {
        closeHelper.verifyNotClosed();
        return defaultNameSpace;
    }

    public INameSpace getPersistentNameSpace() throws DataSpaceException {
        closeHelper.verifyNotClosed();
        return persistentNameSpace;
    }

    public int getPreferredSegmentSize() throws DataSpaceException {
        closeHelper.verifyNotClosed();
        return preferredSegmentSize;
    }

    public INameSpace getRamNameSpace() throws DataSpaceException {
        closeHelper.verifyNotClosed();
        return ramNameSpace;
    }

    public INameSpace getTempNameSpace() {
        return tempNameSpace;
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    // INameSpace methods ----------------------------------------------------------------------------------------------

    @Override
    public PersistenceType getDefaultPersistenceType() {
        return defaultPersistenceType;
    }

    @Override
    public IDataStore getDataStore() {
        return dataStore;
    }

    /**
     * validates the naming convention and that there is no other variable of this name in the name space
     *
     * @param candidateName
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    public String validateName(String candidateName) throws DataSpaceException {
        return defaultNameSpace.validateName(getVariable(candidateName));
    }

    /**
     * Gets the name space part of a dot separated name.
     * "my.name" => "my"
     * "name" => ""
     * @param name
     * @return
     */
    private String getNameSpace(String name) {
        String var = getVariable(name);
        return StringUtils.removeEnd(StringUtils.removeEnd(name,var),".");
    }

    /**
     * Gets the variable space part of a dot separated name.
     * "my.name" => "name"
     * "name" => "name"
     * @param name
     * @return
     */
    private String getVariable(String name) {
        String[] parts = name.split("\\.",2);
        if (parts.length==1) {
            return name;
        }
        return parts[1];
    }

    /**
     * Adds the data element to the name space and promotes it to the default persistence type if it was in a lower persistence type
     *
     * @param name
     * @param value
     */
    @Override
    public void assign(String name, IDataElement value) throws DataSpaceException {
        notifyNameSpaceChangeStart(null);
        try {
            removeFromExistingNameSpace(value);
            DataBundle namespace = getDataBundleFor(name);
            if (namespace==null) {
                throw new DataSpaceException("No such name space: \""+name+"\"");
            }
            String variable = getVariable(name);
            namespace.assign(variable, value);
            ((AbstractDataElement)value).setNameSpace(namespace);
            ((AbstractDataElement)value).setName(namespace.getName()+"."+variable);

            PersistenceType current = value.getPersistenceType();
            PersistenceType namespaceDefaultPersistenceType = namespace.getDefaultPersistenceType();
            if (current == null || namespaceDefaultPersistenceType.compareTo(current) > 0) {
                promotePersistenceType(value, namespaceDefaultPersistenceType);
            }
            notifyNameSpaceChangeFinish(null);
        }
        catch (DataSpaceException e) {
            notifyNameSpaceChangeFailed(null,e);
            throw e;
        }
    }

    @Override
    public List<String> getAssignedVariableNames() {
        return defaultNameSpace.getAssignedVariableNames();
    }

    private void promotePersistenceType(IDataElement value, PersistenceType persistenceType) {
        value.setPersistenceType(persistenceType);
        if (AbstractVector.class.isAssignableFrom(value.getClass())) {
            ((AbstractVector)value).setPersistenceTypeOnAllSegments(persistenceType);
        }
    }

    private DataBundle getDataBundleFor(String name) throws DataSpaceException {
        String ns = getNameSpace(name);
        if (StringUtils.isBlank(ns)) {
            return defaultNameSpace;
        }
        defaultNameSpace.validateName(ns);
        String builtIn = "__"+ns;
        if (dataBundles.containsKey(builtIn)) {
            return dataBundles.get(builtIn);
        }
        return dataBundles.get(ns);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Adds a value without assigning it a name. It will actually end up with a name, but that is unknown to the caller.
     * Really only useful in non-persistent name spaces and therefore persistent values are prohibited.
     * Note that you can get around this, but you don't want to. If you want persistent items it is best to assign them
     * to a persistent name space.
     *
     * @param anonymous
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *          if the data element was persistent
     */
    @Override
    public String add(IDataElement anonymous) throws DataSpaceException {
        notifyNameSpaceChangeStart(null);
        try {
            removeFromExistingNameSpace(anonymous);
            String name = defaultNameSpace.add(anonymous);
            ((AbstractDataElement)anonymous).setNameSpace(defaultNameSpace);
            ((AbstractDataElement)anonymous).setName(defaultNameSpace.getName()+"."+name);
            notifyNameSpaceChangeFinish(null);
            return name;
        }
        catch (DataSpaceException e) {
            notifyNameSpaceChangeFailed(null,e);
            throw e;
        }
    }

    private void removeFromExistingNameSpace(IDataElement anonymous) throws DataSpaceException {
        // HOLD (fix before release)  need to make sure that any persistent values are properly managed - may need to be deleted or renamed. Also make sure that if there is any swapped out pieces - they are properly managed too - see #IT-476
        INameSpace nameSpace = anonymous.getNameSpace();
        if (nameSpace==null) {
            return;
        }
        String name = getVariable(((AbstractDataElement) anonymous).getName());
        nameSpace.remove(name);
    }

    /**
     * Retrieves a variable from the name space
     *
     * @param name
     * @return
     */
    @Override
    public IDataElement get(String name) throws DataSpaceException {
        closeHelper.verifyNotClosed();
        DataBundle namespace = getDataBundleFor(name);
        if (namespace==null) {
            throw new DataSpaceException("No such name space: \""+name+"\"");
        }
        return namespace.get(getVariable(name));
    }

    @Override
    public boolean hasVariable(String name) throws DataSpaceException {
        closeHelper.verifyNotClosed();
        DataBundle namespace = getDataBundleFor(name);
        if (namespace==null) {
            throw new DataSpaceException("No such name space: \""+name+"\"");
        }
        return namespace.hasVariable(name);
    }

    /**
     * Removes the element from the namespace and if the element was persistent it is demoted to temporary
     *
     * @param name
     * @return
     */
    @Override
    public IDataElement remove(String name) throws DataSpaceException {
        notifyNameSpaceChangeStart(null);
        try {
            DataBundle namespace = getDataBundleFor(name);
            if (namespace==null) {
                throw new DataSpaceException("No such name space: \""+name+"\"");
            }
            IDataElement retval = namespace.remove(name);
            notifyNameSpaceChangeFinish(null);
            return retval;
        }
        catch (DataSpaceException e) {
            notifyNameSpaceChangeFailed(null,e);
            throw e;
        }
    }

    @Override
    public void removeDataFrameAndAllVectors(String name) throws DataSpaceException {
        notifyNameSpaceChangeStart(null);
        try {
            DataBundle namespace = getDataBundleFor(name);
            if (namespace==null) {
                throw new DataSpaceException("No such name space: \""+name+"\"");
            }
            namespace.removeDataFrameAndAllVectors(name);
            notifyNameSpaceChangeFinish(null);
        }
        catch (DataSpaceException e) {
            notifyNameSpaceChangeFailed(null,e);
            throw e;
        }
    }

    @Override
    public void remove(IDataElement value) throws DataSpaceException {
        throw new DataSpaceException("Remove by value is only implemented directly on individual data bundles");
    }

    public IMemoryManager getMemoryManager() {
        return memoryManager;
    }


    // Other methods ---------------------------------------------------------------------------------------------------


    public int getDefaultResolutionRangeDivisor() {
        return defaultResolutionRangeDivisor;
    }

    protected void setDefaultResolutionRangeDivisor(int defaultResolutionRangeDivisor) {
        this.defaultResolutionRangeDivisor = defaultResolutionRangeDivisor;
    }

    /**
     * Handles persistence promotions and demotions as variables move between data spaces
     * @param value
     * @param originalNameSpace
     * @param newNameSpace
     */
    protected void finishVariableMove(IDataElement value, DataBundle originalNameSpace, DataBundle newNameSpace) throws DataSpaceException {
        closeHelper.verifyNotClosed();
        if (! AbstractVector.class.isAssignableFrom(value.getClass())) {
            return; // we are only concerned with vectors
        }
        AbstractVector vector = (AbstractVector)value;

        if (originalNameSpace==null) {
            if (vector.getPersistenceType() == PersistenceType.TEMPORARY && newNameSpace.isPersistent()) {
                dataStore.moveAllSegments(vector, true, false);
            }
            vector.setPersistenceTypeOnAllSegments(newNameSpace.getDefaultPersistenceType());
            return;
        }

        if (originalNameSpace.isTemporary() && newNameSpace.isPersistent()) {
            dataStore.moveAllSegments(vector, true, false);
        }
        else if (originalNameSpace.isPersistent() && newNameSpace.isTemporary()) {
            dataStore.moveAllSegments(vector, false, true);
        }
        else if (!originalNameSpace.isTemporary() && !originalNameSpace.isPersistent()) {
            // originally memory only
            dataStore.dumpAllSegments(vector);
        }
        else {
            throw new DataSpaceException("Unsupported value migration from one data space to another: "+originalNameSpace.getName()+" to "+newNameSpace.getName());
        }
        vector.setPersistenceTypeOnAllSegments(newNameSpace.getDefaultPersistenceType());
    }


    /**
     * Called when a namespace is about to change. This may or may not be in the context of an already known change in
     * progress
     * @param nameSpace
     */
    protected void notifyNameSpaceChangeStart(DataBundle nameSpace) throws DataSpaceException {
        closeHelper.verifyNotClosed();
        // concurrency HOLD (fix before release)  need to obtain a write lock here - may not be needed see #IT-477
        changesInProgress.incrementAndGet();
    }

    /**
     * Called when a name space change has finished. This may or may not be in the context of an already known change in
     * progress
     * @param dataBundle
     */
    protected void notifyNameSpaceChangeFinish(DataBundle dataBundle) throws DataSpaceException {
        // concurrency HOLD (fix before release)  need to release a write lock here - may not be needed see #IT-477
        int pending = changesInProgress.decrementAndGet();
        if (pending>0) {
            conditionalFlushIfEnoughTimePassed(); // safety if we messed up the change tracking
            return; // wait until it drops to zero, and then save
        }
        if (pending<0) {
            throw new DataSpaceException("BUG! arrived at notifyNameSpaceChangeFinish("+(dataBundle==null?"data space":dataBundle.getName())+" with the value of changesInProgress="+changesInProgress);
        }
        flush();
    }

    /**
     * Called when a name space change failed with an exception
     * @param dataBundle
     * @param e
     */
    protected void notifyNameSpaceChangeFailed(DataBundle dataBundle, Exception e) {
        // concurrency HOLD (fix before release)  need to release a write lock here - may not be needed see #IT-477
        changesInProgress.decrementAndGet(); // decrement, but do not save
        if (changesInProgress.intValue() < 0) {
            changesInProgress.set(0); // make sure that the next cycle starts clean
        }
    }

    private void flush() throws DataSpaceException {
        dataStore.dump(this);
        lastFlush.set(System.currentTimeMillis());
    }

    private void conditionalFlushIfEnoughTimePassed() throws DataSpaceException {
        long lastFlush = this.lastFlush.get();
        long elapsed = System.currentTimeMillis() - lastFlush;
        if (elapsed > 1000) {
            flush();
        }
    }


    public void close() {
        closeHelper.close();
    }

    public boolean isClosed() {
        return closeHelper.isClosed();
    }

    public int getCode(String s) {
        Integer retval = stringEncoding.get(s);
        if (retval==null) {
            retval = stringDecoding.size();
            stringDecoding.put(retval,s);
            stringEncoding.put(s,retval);
        }
        return retval;
    }

    public String decodeToString(int code) {
        return stringDecoding.get(code);
    }

    public void dumpSummary() throws DataSpaceException, FileNotFoundException {
        dataStore.dumpDataSpaceSummary(this);
    }

    private void deleteSummary() {
        dataStore.deleteSummary(this);
    }

    /**
     * <b>Remove all references to elements in the temporary and persistent namespaces.</b>
     * This operation is part of a overall wipe operation and should not be attempted in parallel to any other activity.
     */
    public synchronized void wipePersistentElementReferences() throws DataSpaceException {
        ArrayList<DataBundle> toClean = new ArrayList<DataBundle>(dataBundles.values());
        toClean.add(persistentNameSpace);
        toClean.add(tempNameSpace);

        for (DataBundle ns: toClean) {
            notifyNameSpaceChangeStart(ns);
            try {
                if (ns.isPersistent() || ns.isTemporary()) {
                    ns.wipeClean();
                }
                notifyNameSpaceChangeFinish(ns);
            }
            catch (DataSpaceException dse) {
                notifyNameSpaceChangeFailed(ns, dse);
                close();
                throw new DataSpaceException("Exception while wiping clean. Closed data space as it is probably useless now. "+dse, dse);
            }
        }
    }

    /**
     * Called after wipe operation is complete and normal operations should resume
     */
    public void onWipeComplete() throws DataSpaceException {
        flush();
        deleteSummary();
        //FIXME implement DataSpace.onWipeComplete
    }
}
