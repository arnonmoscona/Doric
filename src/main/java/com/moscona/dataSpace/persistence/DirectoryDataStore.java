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

import com.google.common.collect.ConcurrentHashMultiset;
import com.moscona.exceptions.InvalidStateException;
import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.segment.AbstractVectorSegment;
import com.moscona.dataSpace.util.UndocumentedJava;
import com.moscona.util.monitoring.stats.IStatsService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created: 1/7/11 11:44 AM
 * By: Arnon Moscona
 */
@SuppressWarnings({"FinalizeDeclaration"})
public class DirectoryDataStore implements IDataStore {
    public static final String METADATA_FILE_NAME = "store.metadata.yml";
    public static final String METADATA_KEY_FORMAT = "format";
    public static final String METADATA_KEY_VERSION = "version";
    public static final String METADATA_KEY_IS_EMPTY = "isEmpty";
    public static final String METADATA_KEY_SEGMENT_SIZE = "segmentSize";
    public static final String FORMAT = "giantSpacePlusBackingSegments";
    public static final String VERSION = "1.0";
    public static final String TRUE_STRING = "true";
    public static final String FALSE_STRING = "false";
    public static final String LOCK_FILE_NAME = "access.lock";
    public static final String TIMING_DIRECTORY_DATA_STORE_CONSTRUCTOR = "DirectoryDataStore()";
    public static final String TIMING_GET_NEXT_SEGMENT_ID = "DirectoryDataStore.getNextSegmentId()";
    public static final String TEMPORARY_SEGMENT_EXT = ".tmp";
    public static final String TIMING_DUMP_DATA_SPACE = "DirectoryDataStore.dump(DataSpace)";
    public static final String TIMING_MOVE_ALL_SEGMENTS = "DirectoryDataStore.moveAllSegments()";
    public static final String TIMING_LOAD_DATA_SPACE = "DirectoryDataStore.loadDataSpace()";
    public static final String TIMING_CLOSE = "DirectoryDataStore.close()";
    public static final String TIMING_COLLECT_GARBAGE = "DirectoryDataStore.collectGarbage()";
    public static final String TIMING_DUMP_SEGMENT = "DirectoryDataStore.dumpSegment()";
    public static final String TIMING_RESTORE_SEGMENT = "DirectoryDataStore.restoreSegment()";

    private String path;
    private boolean isWritable;
    private HashMap<String,String> metadata;
    private Integer segmentSize = null;
    private IStatsService stats; // used to track cost

    // lock support
    FileLock storeLock = null;
    FileOutputStream lockFileOutputStream = null;
    FileInputStream lockFileInputStream = null;
    FileChannel lockFileChannel = null;
    private boolean isOpen=false;
    private String name; // a tag used mostly for debugging
    private static ConcurrentHashMultiset<String> jvmSharedLocks = ConcurrentHashMultiset.create();

    private ArrayList<WeakReference<DataSpace>> referencingDataSpaces;
    private ArrayList<WeakReference<IVector>> referencingVectors;

    public DirectoryDataStore(String rootPath, boolean forWrite, IStatsService stats, String name) throws DataSpaceException, InvalidStateException {
        this.name = ""+name;
        this.stats = stats;
        stats.startTimerFor(TIMING_DIRECTORY_DATA_STORE_CONSTRUCTOR);
        try {
            if (stats==null) {
                throw new DataSpaceException("A stats service is required for cost tracking");
            }
            path = rootPath;
            isWritable = forWrite;
            ensureDirExists(path);
            ensureMetadataIsCompatible();
            obtainFileLock();

            referencingVectors = new ArrayList<WeakReference<IVector>>();
            referencingDataSpaces = new ArrayList<WeakReference<DataSpace>>();

            isOpen = true;
        }
        finally {
            stopTimerFor(TIMING_DIRECTORY_DATA_STORE_CONSTRUCTOR);
        }
    }

    public DirectoryDataStore(String rootPath, boolean forWrite, IStatsService stats) throws DataSpaceException, InvalidStateException {
        this(rootPath, forWrite, stats, "");
    }

    @Override
    protected void finalize() throws Throwable {
        isOpen = false;
        close();
        super.finalize();
    }

    @Override
    public synchronized void close() throws DataSpaceException {
        try {
            stats.startTimerFor(TIMING_CLOSE);
            try {
                if (isOpen) {
                    // unclear how to know which referencing data spaces need to have their summaries dumped and whether they are all identical in structure. So for now this decision is left up to the user code, and not every data space will have a text summary...
                    releaseFileLock();
                    closeAllReferencingVectors();
                    closeAllReferencingDataSpaces();
                    collectGarbage();
                }
                isOpen = false;
            }
            finally {
                stopTimerFor(TIMING_CLOSE);
            }
        }
        catch (IOException e) {
            throw new DataSpaceException("Exception in close: "+e,e);
        }
    }

    private void releaseFileLock() throws IOException {
        if (!isWritable) {
            unRegisterSharedLock(lockFilePath());
        }
        if (storeLock != null) {
            storeLock.release();
        }
        if (lockFileOutputStream != null) {
            lockFileOutputStream.close();
        }
        if (lockFileInputStream != null) {
            lockFileInputStream.close();
        }
    }

    /**
     * deletes all the temporary segments
     */
    private void collectGarbage() throws DataSpaceException {
        // HOLD (fix before release)  see issue #IT-476 about issues with removal from persistent name space
        if (!isWritable) {
            return;
        }
        stats.startTimerFor(TIMING_COLLECT_GARBAGE);
        try {
            File segmentsDir = new File(segmentsDirName());
            if (segmentsDir.exists()) {
                deleteAllTmpFilesIn(segmentsDir, 100);
            }
        }
        finally {
            stopTimerFor(TIMING_COLLECT_GARBAGE);
        }
    }

    private void deleteAllTmpFilesIn(File segmentsDir, int depthCharge) throws DataSpaceException {
        if (depthCharge<=0) {
            throw new DataSpaceException("Directory structure too deep. There is some problem. Aborting garbage collection");
        }
        File[] files = segmentsDir.listFiles();
        for (File f: files) {
            if (f.isDirectory()) {
                deleteAllTmpFilesIn(f, depthCharge-1);
            }
            else {
                if (f.getName().endsWith(TEMPORARY_SEGMENT_EXT)) {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
        }
    }

    private void closeAllReferencingDataSpaces() {
        for (WeakReference<DataSpace> dsReference: referencingDataSpaces) {
            DataSpace ds = dsReference.get();
            if(ds!=null) {
                //still exists
                ds.close();
            }
        }
    }

    private void closeAllReferencingVectors() {
        for (WeakReference<IVector> vectorReference: referencingVectors) {
            IVector vector = vectorReference.get();
            if (vector!=null) {
                // still exists - close it (will swap out all segments)
                ((AbstractVector) vector).close();
            }
        }
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Tries to obtain an appropriate lock for the whole store. An exclusive lock if opened for write or a shared lock
     * if opened for read.
     * IMPORTANT if you have a crashed process with an open lock then the file may not be deletable any longer. You might need to resort to Unlocker (http://download.cnet.com/Unlocker/3000-2248_4-10493998.html?tag=mncol;1) - which might have security risks...
     */
    @SuppressWarnings({"ChannelOpenedButNotSafelyClosed", "IOResourceOpenedButNotSafelyClosed"})
    private synchronized void obtainFileLock() throws DataSpaceException {
        try {
            if (isWritable) {
                // obtain an exclusive (write) lock on the lock file
                lockFileOutputStream = new FileOutputStream(lockFilePath()); // IMPORTANT these are not closed to maintain the file system lock on the lock file until the store is closed
                lockFileChannel = lockFileOutputStream.getChannel(); // IMPORTANT these are not closed to maintain the file system lock on the lock file until the store is closed
                storeLock = lockFileChannel.tryLock(0, Long.MAX_VALUE, !isWritable); // you can only obtain a write lock on a writable stream and a read lock on a readable stream
                String pid = new UndocumentedJava().pid()+"\n"+new Date()+"\nwrite locked\n";
                lockFileOutputStream.write(pid.getBytes());
                lockFileOutputStream.flush();
            }
            else {
                // obtain a shared (read) lock on the lock file
                // IMPORTANT - this is nasty - I will be denied shared locks within the same JVM!! Meaning - I can share with other processes, but not with myself. Therefore I must track all the read locks I have globally by using a static member. That is indeed nasty!
                if (jvmAlreadyHasASharedLockOn(lockFilePath())) {
                    registerSharedLock(lockFilePath());
                    return; // no need to lock, and if I try I will get an overlapping lock exception even though it is a shared lock
                }
                lockFileInputStream = new FileInputStream(lockFilePath());
                lockFileChannel = lockFileInputStream.getChannel();
                storeLock = lockFileChannel.tryLock(0, Long.MAX_VALUE, !isWritable); // you can only obtain a write lock on a writable stream and a read lock on a readable stream
                registerSharedLock(lockFilePath());
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while trying to obtain a lock for "+path+": "+e,e);
        }

        if (storeLock==null) {
            throw new DataSpaceException("Failed to obtain a file system lock on "+lockFilePath()+(isWritable?" (exclusive/write)":" (shared/read)"));
        }
    }

    private synchronized void registerSharedLock(String path) {
        jvmSharedLocks.add(path);
    }

    private synchronized void unRegisterSharedLock(String path) {
        jvmSharedLocks.remove(path);
    }

    private synchronized boolean jvmAlreadyHasASharedLockOn(String path) {
        return jvmSharedLocks.count(path)>0;
    }

    private void ensureMetadataIsCompatible() throws DataSpaceException {
        if (isEmptyDir(new File(path))) {
            // create a new store in this location
            create();
        }
        else {
            loadMetaDataFile();
        }
    }

    private void create() throws DataSpaceException {
        createNewMetadata();
        dumpMetaDataFile();
        createSegmentIDFile();
        createLockFile();
    }

    private void createSegmentIDFile() throws DataSpaceException {
        try {
            File segmentIdFile = getSegmentIdFile();
            PrintStream printStream = new PrintStream(segmentIdFile);
            try {
                printStream.println(0);
            }
            finally {
                printStream.close();
            }
        }
        catch (FileNotFoundException e) {
            throw new DataSpaceException("Exception while initializing segment ID file: "+e,e);
        }

    }

    private int getNextSegmentId() throws DataSpaceException, InvalidStateException {
        stats.startTimerFor(TIMING_GET_NEXT_SEGMENT_ID);
        markNotEmpty();
        int retval = -1;
        try {
            File file = getSegmentIdFile();
            String lastId = FileUtils.readFileToString(file).trim();
            retval = Integer.parseInt(lastId)+1;
            FileUtils.writeStringToFile(file,Integer.toString(retval));
        }
        catch (IOException e) {
            throw new DataSpaceException("IO exception while trying to obtain last segment ID: "+e,e);
        }
        finally {
            stopTimerFor(TIMING_GET_NEXT_SEGMENT_ID);
        }

        return retval;
    }

    private File getSegmentIdFile() {
        return new File(path+"/"+"max.segment.id");
    }

    private void createLockFile() throws DataSpaceException {
        try {
            FileUtils.touch(new File(lockFilePath()));
        }
        catch (IOException e) {
            throw new DataSpaceException("Exception while trying to create a lock file "+lockFilePath());
        }
    }

    private String lockFilePath() {
        return path + "/" + LOCK_FILE_NAME;
    }

    @SuppressWarnings({"unchecked"})
    private void loadMetaDataFile() throws DataSpaceException {
        File metadataFile = new File(path+"/"+METADATA_FILE_NAME);
        try {
            if (metadataFile.exists()) {
                String yaml = FileUtils.readFileToString(metadataFile);
                metadata = (HashMap<String,String>) (new Yaml().load(yaml)); // unchecked cast
                if (metadata.containsKey(METADATA_KEY_SEGMENT_SIZE)) {
                    segmentSize = Integer.parseInt(metadata.get(METADATA_KEY_SEGMENT_SIZE));
                }
            } else {
                createNewMetadata();
                dumpMetaDataFile();
                if (! getSegmentIdFile().exists()) {
                    createSegmentIDFile();
                }
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while loading metadata file "+metadataFile.getAbsolutePath()+": "+e,e);
        }
    }

    private void dumpMetaDataFile() throws DataSpaceException{
        File metadataFile = new File(path+"/"+METADATA_FILE_NAME);
        try {
            PrintStream out = new PrintStream(metadataFile);
            try {
                String yaml = new Yaml().dump(metadata);
                out.println(yaml);
            }
            finally {
                out.close();
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while dumping metadata file "+metadataFile.getAbsolutePath()+": "+e,e);
        }
    }

    private void markNotEmpty() throws DataSpaceException {
        if (metadata.get(METADATA_KEY_IS_EMPTY).equals(TRUE_STRING)) {
            metadata.put(METADATA_KEY_IS_EMPTY, FALSE_STRING);
            dumpMetaDataFile();
        }
    }

    private void createNewMetadata() {
        metadata = new HashMap<String,String>();
        metadata.put(METADATA_KEY_FORMAT, FORMAT);
        metadata.put(METADATA_KEY_VERSION, VERSION);
        metadata.put(METADATA_KEY_IS_EMPTY, TRUE_STRING);
    }

    private boolean isEmptyDir(File dir) throws DataSpaceException {
        ensureIsUsableDirectory(dir);
        return dir.list().length == 0;
    }

    private void ensureDirExists(String filePath) throws DataSpaceException {
        File dir = new File(filePath);
        if (!dir.exists()) {
            if (! dir.mkdirs()) {
                throw new DataSpaceException("Failed to create directory for data store at: "+filePath);
            }
        }
        ensureIsUsableDirectory(dir);
    }

    private void ensureIsUsableDirectory(File dir) throws DataSpaceException {
        String filePath = dir.getAbsolutePath();

        if (!dir.exists()) {
            throw new DataSpaceException("The directory does not exist: "+filePath);
        }
        if (!dir.isDirectory()) {
            throw new DataSpaceException("The specified path is not a directory: "+filePath);
        }
        if (!dir.canRead()) {
            throw new DataSpaceException("Cannot read "+filePath);
        }
        if (!dir.canWrite()) {
            throw new DataSpaceException("Cannot write to "+filePath);
        }
    }

    @Override
    public boolean isEmpty() {
        return metadata.get(METADATA_KEY_IS_EMPTY).equals(TRUE_STRING);
    }

    @Override
    public Integer getSegmentSize() {
        return segmentSize;
    }

    @Override
    public void setSegmentSize(int size) throws DataSpaceException {
        if (segmentSize != null) {
            if (segmentSize !=size) {
                throw new DataSpaceException("Segment size already set to "+segmentSize+", cannot change it to "+size);
            }
        }
        else {
            segmentSize = size;
            metadata.put(METADATA_KEY_SEGMENT_SIZE, segmentSize.toString());
            dumpMetaDataFile();
        }

    }

    @Override
    public void dumpSegment(IVectorSegment segment) throws DataSpaceException {
        stats.startTimerFor(TIMING_DUMP_SEGMENT);
        try {
            AbstractVectorSegment abstractSegment = (AbstractVectorSegment)segment;
            SegmentFileInfo fileInfo = new SegmentFileInfo(segment);

            File file = new File(fileInfo.filePath);
            File parent = file.getParentFile();
            ensureDirExists(parent.getAbsolutePath());

            IVectorSegmentBackingArray backingArray = abstractSegment.getBackingArray();
            try {
                OutputStream out = new FileOutputStream(file);
                try {
                    ObjectOutput objOut = new ObjectOutputStream(new GZIPOutputStream(out));
                    try {
                        objOut.writeObject(backingArray);
                    }
                    finally {
                        objOut.close();
                    }
                }
                finally {
                    out.close();
                }
            }
            catch (Exception e) {
                throw new DataSpaceException("Exception while saving backing array to "+fileInfo.filePath+": "+e,e);
            }
            markSwappedIn(abstractSegment);
        }
        catch (InvalidStateException e) {
            throw new DataSpaceException("Exception while dumping segment: "+e,e);
        }
        finally {
            stopTimerFor(TIMING_DUMP_SEGMENT);
        }
    }

    private void markSwappedIn(AbstractVectorSegment abstractSegment) throws DataSpaceException {
        abstractSegment.setPersistenceStatus(PersistenceStatus.SWAPPED_IN);
        abstractSegment.getDataSpace().getMemoryManager().submit(abstractSegment);
    }

    @Override
    @SuppressWarnings({"unchecked"}) // setting the backing array without checking type compatibility
    public void restoreSegment(IVectorSegment segment) throws DataSpaceException, InvalidStateException {
        AbstractVectorSegment abstractSegment = (AbstractVectorSegment)segment;
        SegmentFileInfo fileInfo = new SegmentFileInfo(segment);
        IVectorSegmentBackingArray backingArray = null;
        stats.startTimerFor(TIMING_RESTORE_SEGMENT);
        try {
            InputStream in = new FileInputStream(fileInfo.filePath);
            try {
                ObjectInputStream objIn = new ObjectInputStream(new GZIPInputStream(in));
                try {
                    backingArray = (IVectorSegmentBackingArray)objIn.readObject();
                    abstractSegment.setBackingArray(backingArray); // unchecked call
                    markSwappedIn(abstractSegment);
                }
                finally {
                    objIn.close();
                }
            }
            finally {
                in.close();
            }
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while restoring backing array from "+fileInfo.filePath+": "+e,e);
        }
        finally {
            stopTimerFor(TIMING_RESTORE_SEGMENT);
        }
    }

    @Override
    public void moveAllSegments(IVector vector, boolean fromTemporary, boolean toTemporary) throws DataSpaceException {
        stats.startTimerFor(TIMING_MOVE_ALL_SEGMENTS);
        try {
            if (fromTemporary==toTemporary) {
                return; // nothing to do
            }
            if (! AbstractVector.class.isAssignableFrom(vector.getClass())) {
                throw new DataSpaceException("This implementation only knows how to deal with AbstractVector instances. Got "+vector.getClass().getName());
            }

            AbstractVector.SegmentIterator iterator = ((AbstractVector) vector).segmentIterator();
            while (iterator.hasNext()) {
                AbstractVector.SegmentInfo segment = iterator.next();
                String location = ((AbstractVectorSegment) segment.getSegment()).getBackingArrayStorageLocation();
                if (location==null) {
                    throw new DataSpaceException("Found a segment with no existing file location in moveAllSegments()! Segment #"+segment.getSegmentNumber()+" Assume that the vector storage is now corrupt!");
                }
                String newLocation = toTemporary ? location+TEMPORARY_SEGMENT_EXT : StringUtils.removeEnd(location, TEMPORARY_SEGMENT_EXT);
                boolean success = new File(location).renameTo(new File(newLocation));
                if (!success) {
                    throw new DataSpaceException("Failed to rename segment file in moveAllSegments()! Segment #"+segment.getSegmentNumber()+" Assume that the vector storage is now corrupt! Segment file: "+location);
                }
                ((AbstractVectorSegment) segment.getSegment()).setBackingArrayStorageLocation(newLocation);
            }
        }
        finally {
            stopTimerFor(TIMING_MOVE_ALL_SEGMENTS);
        }
    }

    @Override
    public void dumpAllSegments(IVector vector) throws DataSpaceException {
        markNotEmpty();
        if (! AbstractVector.class.isAssignableFrom(vector.getClass())) {
            throw new DataSpaceException("This implementation only knows how to deal with AbstractVector instances. Got "+vector.getClass().getName());
        }
        AbstractVector.SegmentIterator iterator = ((AbstractVector) vector).segmentIterator();
        while (iterator.hasNext()) {
            AbstractVector.SegmentInfo segment = iterator.next();
            dumpSegment(segment.getSegment());
        }
    }

    @Override
    public void dump(DataSpace dataSpace) throws DataSpaceException {
        stats.startTimerFor(TIMING_DUMP_DATA_SPACE);
        String file = dataSpaceFileName();
        try {
            OutputStream out = new FileOutputStream(file);
            try {
                ObjectOutput objOut = new ObjectOutputStream(out);
                try {
                    objOut.writeObject(dataSpace);
                }
                finally {
                    objOut.close();
                }
            }
            finally {
                out.close();
            }
            //dumpDataSpaceSummary(dataSpace);
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while saving data space "+file+": "+e,e);
        }
        finally {
            stopTimerFor(TIMING_DUMP_DATA_SPACE);
        }
    }

    @Override
    public void deleteSummary(DataSpace dataSpace) {
        File file = new File(dataSpaceSummaryFileName());
        if (file.exists()) {
            file.delete();
        }
    }

    @Override
    public void dumpDataSpaceSummary(DataSpace dataSpace) throws FileNotFoundException, DataSpaceException {
        stats.startTimerFor("dumpDataSpaceSummary");
        PrintStream out = new PrintStream(new FileOutputStream(dataSpaceSummaryFileName()));
        try {
            out.println("Persistent name space");
            INameSpace ns = dataSpace.getPersistentNameSpace();
            List<String> vars = ns.getAssignedVariableNames();
            HashSet<IDataElement> visited = new HashSet<IDataElement>();

            out.println();
            out.println("List of variables:");
            out.println();
            for (String var: vars) {
                IDataElement element = ns.get(var);
                out.println("  "+var+": "+element.getClass().getSimpleName()+"  Description: "+element.getDescription());
            }

            out.println();
            out.println();
            out.println("Details:");
            out.println();
            out.println();

            for (String var: vars) {
                IDataElement element = ns.get(var);
                visited.add(element);
                out.println();
                out.println("Variable: \""+var+"\": "+element.getClass().getSimpleName());
                out.println("  Description: "+element.getDescription());
                if (IScalar.class.isAssignableFrom(element.getClass())) {
                    dumpSummary((IScalar) element, out);
                }
                else if (element instanceof AbstractVector) {
                    dumpSummary((AbstractVector) element, out, " ");
                }
                else if (element instanceof DataFrame) {
                    dumpSummary((DataFrame) element, out, visited);
                }
                else {
                    out.println("  no summary in for available for "+element.getClass());
                }
            }
        }
        finally {
            try {
                stats.stopTimerFor("dumpDataSpaceSummary");
            }
            catch (InvalidStateException e) {
                // do nothing
            }
            out.close();
        }
    }

    private void dumpSummary(DataFrame element, PrintStream out, HashSet<IDataElement> visited) throws DataSpaceException {
        Set<String> columns = element.getColumnNames();
        ArrayList<String> labels = new ArrayList<String>();
        ArrayList<String> factors = new ArrayList<String>();
        ArrayList<String> others = new ArrayList<String>();
        String id = element.getRowId();
        if (id!=null) {
            out.println("  ID column: "+id);
            factors.add(id);
        }
        for (String col: columns) {
            IVector c = element.get(col);
            visited.add(c);
            if (element.isLabel(col)) {
                labels.add(col);
            }
            else if(c.isFactor() && !factors.contains(col)) {
                factors.add(col);
            }
            else {
                others.add(col);
            }
        }

        Collections.sort(labels);
        Collections.sort(factors);
        Collections.sort(others);

        out.println("  columns:");
        for (String col: factors) {
            IVector vector = element.get(col);
            String description = vector.getDescription();
            out.println("    " + col + " " + (vector.isSorted() ? "(factor, sorted)" : "(factor)") + (description==null?"":": " + description));
        }
        for (String col: others) {
            IVector vector = element.get(col);
            String description = vector.getDescription();
            out.println("    " + col + " " + (vector.isSorted() ? "(sorted)" : "") + (description==null?"":": " + description));
        }
        for (String col: labels) {
            IVector c = element.get(col);
            String description = c.getDescription();
            out.println("    " + col + " (label" + (c.isFactor() ? ", factor" : "") + (c.isSorted() ? ", sorted)" : ")")+(description==null?"":": " + description));
        }


        ArrayList<String> order = new ArrayList<String>(factors);
        order.addAll(others);
        order.addAll(labels);

        out.println();
        out.println("  data frame column details:");
        for (String col: order) {
            AbstractVector vector = (AbstractVector) element.get(col);
            String description = vector.getDescription();
            out.println();
            out.println("    "+col+(description==null?"":": " + description));
            dumpSummary(vector, out, "      ");
        }

    }

    private void dumpSummary(AbstractVector element, PrintStream out, String prefix) throws DataSpaceException {
        out.println(prefix + "size: "+element.size());
        out.println(prefix + "base type: "+element.getBaseType());
        out.println(prefix + "sorted: "+element.isSorted());
        ArrayList<String> samples = new ArrayList<String>();
        for (int i=0; i< Math.min(10,element.size()); i++) {
            samples.add(element.get(i).toString());
        }
        out.println(prefix + "sample data: "+StringUtils.join(samples,", "));
        out.println(prefix + "min: "+element.getStats().getDescriptiveStats().getMin());
        out.println(prefix + "max: "+element.getStats().getDescriptiveStats().getMax());
    }

    private void dumpSummary(IScalar element, PrintStream out) {
        out.println("  value: "+element);
    }

    @Override
    public synchronized void register(DataSpace dataSpace) {
        referencingDataSpaces.add(new WeakReference<DataSpace>(dataSpace));
    }

    @Override
    public synchronized void register(IVector vector) {
        referencingVectors.add(new WeakReference<IVector>(vector));
    }

    private String dataSpaceFileName() {
        return path+"/dataSpace.java.obj";
    }

    private String dataSpaceSummaryFileName() {
        return path+"/dataSpace.manifest.txt";
    }

    public DataSpace loadDataSpace(IMemoryManager memoryManager) throws DataSpaceException {
        stats.startTimerFor(TIMING_LOAD_DATA_SPACE);
        try {
            DataSpace dataSpace = null;
            InputStream in = new FileInputStream(dataSpaceFileName());
            try {
                ObjectInputStream objIn = new ObjectInputStream(in);
                try {
                    dataSpace = (DataSpace)objIn.readObject();
                    dataSpace.initTransientsAfterRestore(this, memoryManager);
                    register(dataSpace);
                }
                finally {
                    objIn.close();
                }
            }
            finally {
                in.close();
            }
            return dataSpace;
        }
        catch (Exception e) {
            throw new DataSpaceException("Exception while restoring data space from "+dataSpaceFileName()+": "+e,e);
        }
        finally {
            stopTimerFor(TIMING_LOAD_DATA_SPACE);
        }
    }

    public synchronized boolean canWrite() {
        return isWritable;
    }

    private String getBackingArrayPathForSegment(int num, String suffix) {
        String str = Integer.toString(num);
        ArrayList<String> parts = new ArrayList<String>();
        while (str.length()>2) {
            String head = StringUtils.left(str,2);
            str = StringUtils.removeStart(str,head);
            parts.add(head);
        }
        return StringUtils.removeEnd(segmentsDirName() +StringUtils.join(parts,"/"),"/")+"/"+Integer.toString(num)+"."+suffix.trim();
    }

    private String segmentsDirName() {
        return path+"/segments/";
    }

    public synchronized void delete() throws DataSpaceException, IOException {
        preDeleteWriteCheck();
        close();
        //  obtain lock
        isWritable = true;
        obtainFileLock();
        // delete everything but the lock
        recursiveDelete(path, 20, false);
        // release lock and delete it
        releaseFileLock();
        boolean deleted = new File(lockFilePath()).delete();
        if (!deleted) {
            throw new DataSpaceException("After deleting directory data store, failed to delete lock file "+lockFilePath());
        }

        // make sure segments directory also is deleted
        File parent = new File(lockFilePath()).getParentFile();
        File segmentsDir = new File(segmentsDirName());
        if (segmentsDir.exists()) {
            if (! segmentsDir.delete()) {
                throw new DataSpaceException("Failed to delete segments directory "+segmentsDirName());
            }
        }
        // delete directory
        deleted = parent.delete();
        if (!deleted) {
            throw new DataSpaceException("After deleting directory data store, failed to delete directory "+parent.getAbsolutePath());
        }
    }

    private void preDeleteWriteCheck() throws DataSpaceException {
        if (! (isOpen && isWritable) ) {
            throw new DataSpaceException("In order to delete a data store, you must first open it for write");
        }
    }

    /**
     * Deletes everything, excluding the lock file and its parent directory
     * @param path
     * @param depthCharge
     * @throws DataSpaceException
     */
    private void recursiveDelete(String path, int depthCharge, boolean deleteDir) throws DataSpaceException {
        if (depthCharge<0) {
            throw new DataSpaceException("Recursive depth exceeded in directory data store recursive delete at "+path);
        }
        File thisFile = new File(path);
        File[] files = thisFile.listFiles();
        String lockPath = new File(lockFilePath()).getAbsolutePath();
        String parentPath = new File(lockFilePath()).getParentFile().getAbsolutePath();

        if (files!=null) {
            for (File file: files) {
                String absolutePath = file.getAbsolutePath();
                if (absolutePath.equals(lockPath) || absolutePath.equals(parentPath)) {
                    continue;
                }
                if (file.isDirectory()) {
                    recursiveDelete(file.getAbsolutePath(), depthCharge-1, true);
                    if (!file.delete() && file.exists()) {
                        throw new DataSpaceException("Failed to delete "+file.getAbsolutePath()+" after deleting its contents");
                    }
                }
                else {
                    boolean deleted = file.delete();
                    if (!deleted) {
                        throw new DataSpaceException("Failed to delete "+file.getAbsolutePath());
                    }
                }
            }
        }
        if (deleteDir && thisFile.isDirectory()) {
            if(! thisFile.delete() && thisFile.exists()) {
                throw new DataSpaceException("Failed to delete "+thisFile.getAbsolutePath()+" after deleting its contents");
            }
        }
    }

    /**
     * Given an *open for write* data store:
     *   - instructs the referencing data spaces to wipe their persistent data element references,
     *   - deletes all the segment files in the data store
     *   - instructs all referencing data spaces to resume normal operation
     * <p/><b>
     *   IMPORTANT: this will cause serious trouble if there are any threads doing absolutely anything with any
     *   associated data space.</b>
     * <p/><b>
     *   WARNING: this operation is fundamentally unsafe and is done in a non-synchronized fashion so as not to add
     *   locking overhead for normal operations while none of this is going on.</b>
     */
    public void wipeClean() throws DataSpaceException {
        preDeleteWriteCheck();

        // collect all real referencing data spaces that exist at this point
        ArrayList<DataSpace> referencing = new ArrayList<DataSpace>();
        for (WeakReference<DataSpace> ref: referencingDataSpaces) {
            DataSpace ds = ref.get();
            if (ds != null) {
                referencing.add(ds);
            }
        }

        // now we have a list of concrete data spaces.
        // Notify them all to wipe references
        for (DataSpace ds: referencing) {
            ds.wipePersistentElementReferences();
        }

        // delete all segment data
        recursiveDelete(segmentsDirName(), 20, false);

        // tell all data spaces to resume normal operation
        for (DataSpace ds: referencing) {
            ds.onWipeComplete();
        }
    }

    public String getRootPath() {
        return path;
    }

    /**
     * A simple value object that concentrates the computed information about the segment backing array and ensures
     * that the segment has the pertinent data in it.
     */
    private class SegmentFileInfo {
        protected boolean isTmp;
        protected String filePath;
        protected int segmentId;

        protected SegmentFileInfo(IVectorSegment segment) throws DataSpaceException, InvalidStateException {
            AbstractVectorSegment abstractSegment = (AbstractVectorSegment)segment;
            PersistenceType persistenceType = segment.getPersistenceType();
            String ext = "";
            if (persistenceType == PersistenceType.TEMPORARY) {
                ext = TEMPORARY_SEGMENT_EXT;
                isTmp = true;
            }
            else {
                isTmp = false;
            }

            if (abstractSegment.getDataStoreSegmentId() == null) {
                segmentId = getNextSegmentId();
                abstractSegment.setDataStoreSegmentId(segmentId);
            }
            else {
                segmentId = abstractSegment.getDataStoreSegmentId();
            }
            String suffix = abstractSegment.getVector().getBaseType().toString();
            filePath = getBackingArrayPathForSegment(segmentId,suffix)+ext;
            abstractSegment.setBackingArrayStorageLocation(filePath);
        }
    }

    private void stopTimerFor(String stat) {
        try {
            stats.stopTimerFor(stat);
        }
        catch (InvalidStateException e) {
            System.err.println("Error while updating stats: "+e);
        }
    }
}
