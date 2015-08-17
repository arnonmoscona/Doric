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

package com.moscona.dataSpace.impl;

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.segment.AbstractSegmentStats;
import com.moscona.dataSpace.impl.segment.AbstractVectorSegment;
import com.moscona.dataSpace.persistence.PersistenceType;
import com.moscona.exceptions.NotImplementedException;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created: 12/10/10 2:27 PM
 * By: Arnon Moscona
 * implements the common vector behavior with respect to segmenting etc.
 */
public abstract class AbstractVector<T extends IScalar> extends AbstractDataElement implements IVector<T>, IVectorBuilder<T> {
    private static final long serialVersionUID = 8416426852225836003L; // incompatible change
    public static final int DEFAULT_MAX_UNIQUE_VALUES = 100000;
    public static final String IS_SORTED = "isSorted";

    private transient DataSpace dataSpace;

    private ArrayList<IVectorSegment<T>> segments = null;
    private ArrayList<AbstractSegmentStats<T>> segmentStats = null;
    private boolean isSealed = false;
    private int segmentSize;
    private int size;
    private IVectorStats<T> stats = null;
    private transient CloseHelper closeHelper;
    private HashMap<String,Object> metaData;

    // factor related
    String factorName; // just the name of the factor, actual factor calculation may be deferred until fetch time
    IFactor factor;
    boolean autoResolution = true;
    double resolution = 0.00001; // arbitrary - it will change anyway
    private PersistenceType minimumPersistenceType = PersistenceType.MEMORY_ONLY;

    protected AbstractVector(DataSpace dataSpace) {
        closeHelper = new CloseHelper();
        isSealed = false;
        size = 0;
        this.segmentSize = dataSpace.getSegmentSize();
        this.dataSpace = dataSpace;
        setPersistenceType(dataSpace.getDefaultPersistenceType());

        segments = new ArrayList<IVectorSegment<T>>();
        segmentStats = new ArrayList<AbstractSegmentStats<T>>();

        factorName = null;
        factor = null;

        metaData = new HashMap<String,Object>();

        dataSpace.getDataStore().register(this);
    }

    /**
     * Creates a new vector in the same data space
     * @return
     */
    protected abstract AbstractVector<T> createNew();

    /**
     * For subclasses to fill in the blank - create a new segment as appropriate
     * @return
     */
    protected abstract IVectorSegment<T> createNewSegment();

    /**
     * Does the append in a subclass using primitive types as appropriate
     * Overall vector size is accumulated in the abstract base class.
     * @param element
     * @throws DataSpaceException
     */
    protected abstract void abstractAppend(T element) throws DataSpaceException;

    /**
     * tells us whether the specific type has stats (e.g. Logical does not)
     * @return
     */
    protected abstract boolean vectorTypeSupportsStats();

    /**
     * gets the value in a specific segment after it has been required
     * @param segment
     * @param segmentIndex
     * @return
     */
    protected abstract T get(IVectorSegment segment, int segmentIndex);

    /**
     * efficient copy of a partial segment
     * @param sourceSegment
     * @param pos
     * @param workingSegment
     * @param start
     * @param length
     */
    protected abstract void copyPartialSegment(IVectorSegment sourceSegment, int pos, IVectorSegment workingSegment, int start, int length) throws DataSpaceException;

    /**
     * Creates the parameter type from a double
     * In vectors that support resolution this must override in vectors that support resolution
     * @param restored
     * @return
     */
    protected T makeT(double restored) {
        return null;  // Must override in vectors that support resolution
    }


    /**
     * Appends all the elements of the other vector to the end of this vector.
     * Note that this is more efficient than append(IVector<T> vector) or append(List<T> vector) as implementations can
     * (and do) use direct access to the backing array.
     *
     * @param vector
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *          if the vector is sealed
     */
    @Override
    public IVector<T> append(IVector<T> vector) throws DataSpaceException {
        requireSealedStatus(false);
        /*
         * The vectors are assumed to have the same implementation.
         * The vectors are assumed to come in mis-aligned.
         * They are also assumed to have the same segment size (otherwise we just degrade to element by element operation).
         * So if we want to get an array copy operation we must do it in chunks.
         * The first chunk size is the remaining space in the working segment.
         * The second chunk size is the (segment size - the first chunk size)
         * We iterate until we run out of stuff to copy.
         */

        AbstractVector<T> other = (AbstractVector<T>)vector;
        if (other.getSegmentSize() != getSegmentSize()) {
            performNaiveVectorAppend(other);
        }

        IVectorSegment lastSegment = getSegmentForIndex(size(), true);
        int chunk2 = lastSegment.size();
        int chunk1 = getSegmentSize() - chunk2;
        int sourceSegmentNo = 0;
        int remainingToCopy = other.size();
        int copied = 0;

        while (remainingToCopy>0) {
            // copy chunk 1
            IVectorSegment sourceSegment = other.getSegment(sourceSegmentNo);
            int copySize = Math.min(chunk1,remainingToCopy); // the actual amount to be copies
            copyPartialSegment(sourceSegment, 0, lastSegment, chunk2, copySize);
            remainingToCopy -= copySize;
            copied += copySize;

            // check if there is anything left to do
            if (remainingToCopy>0) {
                closeLastSegment(); // we just filled it up
                // get a new working segment
                lastSegment = getSegmentForIndex(size()+copied, true);

                // copy chunk 2

                //noinspection ReuseOfLocalVariable
                copySize = Math.min(chunk2, remainingToCopy);
                copyPartialSegment(sourceSegment, chunk1, lastSegment, 0, copySize);
                remainingToCopy -= copySize;
                copied += copySize;

                // advance the source segment number
                sourceSegmentNo++;
            }
        }

        if (copied != other.size()) {
            throw new DataSpaceException("Bug! IVector<T> append(IVector<T> vector) was supposed to copy "+
                    other.size()+" elements but somehow copied "+copied+" instead");
        }

        size += other.size();
        return this;
    }


    /**
     * Performs an element by element copy from one vector to another. This can happen when copying a vector from one
     * data space to another, where the vectors have different sizes.
     * @param other
     */
    private void performNaiveVectorAppend(AbstractVector<T> other) throws DataSpaceException {
        for (int i=0; i<other.size(); i++) {
            T element = other.get(i);
            append(element);
        }
    }

    /**
     * Appends a single element to the vector
     *
     * @param element
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *          if the vector is sealed
     */
    @Override
    public IVector<T> append(T element) throws DataSpaceException {
        abstractAppend(element);
        size++;
        return this;
    }

    protected void incrementSize() {
        size++;
    }


    @Override
    public IVector<T> append(T[] vector) throws DataSpaceException {
        requireSealedStatus(false);
        for (T element: vector) {
            append(element);
        }
        return this;
    }

    @Override
    public IVector<T> append(List<T> vector) throws DataSpaceException {
        requireSealedStatus(false);
        for (T element: vector) {
            append(element);
        }
        return this;
    }

    protected IVectorSegment getSegment(int i) throws DataSpaceException {
        if (segments.size() <= i || i<0) {
            throw new DataSpaceException("No such segment. requested segment #"+i+" but there are only "+segments.size()+
                    " segments in the vector");
        }
        return segments.get(i);
    }

    protected IVectorSegment getSegmentForIndex(int i, boolean createNextIfNeeded) throws DataSpaceException {
        int segmentNo = segmentNo(i);
        if (segmentNo==segments.size() && createNextIfNeeded) {
            requireSealedStatus(false);
            closeLastSegment();
            IVectorSegment<T> segment = createNewSegment(); // call the subclass to create a new segment for us
            segment.setVector(this);
            segment.setSegmentNumber(segmentNo);
            segments.add(segment); // add it to the list
            return segment;
        }
        return getSegment(segmentNo);
    }

    @SuppressWarnings({"unchecked"}) // unchecked cast to AbstractSegmentStats<T>
    private void closeLastSegment() throws DataSpaceException {
        if (segments.size()==0) {
            return; // nothing to do
        }
        IVectorSegment segment = segments.get(segments.size() - 1);
        segment.seal(); // seal the last segment that was still open
        AbstractSegmentStats<T> stats = (AbstractSegmentStats<T>)segment.calculateStats();
        segmentStats.add(stats);
        dataSpace.getDataStore().dumpSegment(segment);
        int id = dataSpace.getMemoryManager().submit(segment);
        segment.setMemoryManagerId(id);  // redundant -  done in the memory manager in the submit method (left in code for now - belt & suspenders)
    }


    protected IVectorSegment getSegmentForIndex(int i) throws DataSpaceException {
        return getSegmentForIndex(i,false);
    }

    @Override
    public boolean isReadyToQuery() {
        return isSealed;
    }

    protected void requireSealedStatus(boolean expected) throws DataSpaceException {
        closeHelper.verifyNotClosed();

        if (isSealed != expected) {
            String err = isSealed ? "Operation not allowed on a sealed vector" : "Operation requires a sealed vector";
            throw new DataSpaceException(err);
        }
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    public int getSegmentCount() {
        return segments.size();
    }

    /**
     * Closes the vector and makes it immutable
     */
    @Override
    public IVector<T> seal() throws DataSpaceException {
        requireSealedStatus(false);
        closeLastSegment();
        calculateVectorStatistics();
        isSealed = true;
        //dataSpace.onVectorSeal(this);
        return this;
    }

    @Override
    public IVectorStats<T> getStats() throws DataSpaceException {
        requireSealedStatus(true);
        return stats;
    }

    /**
     * Calculates overall vector statistics after the vector has been sealed
     */
    private void calculateVectorStatistics() throws DataSpaceException {
        if (vectorTypeSupportsStats()) {
            stats = new VectorStats<T>(); // HOLD calculate some stats on-demand and cache here - see #IT-469
            stats.setDescriptiveStats(aggregateSegmentDescriptiveStats());
            if (isNumeric() && size > 0) {
                if (segmentSize < Quantiles.MARKER_COUNT*2) {
                    // this is really a corner case that in reality is only encountered in testing, but can also happen if somebody loses their wits and makes a data space with a truly tiny segment size
                    stats.setQuantiles(new Quantiles(getEntireVectorAsDoubles()));
                }
                else {
                    Quantiles quantiles = new Quantiles(getFirstSegmentCopyAsDoubles());
                    if (size <= segmentSize) {
                        stats.setQuantiles(quantiles);
                    }
                    else {
                        stats.setQuantiles(estimateQuantilesOnRestOfSegments(quantiles));
                    }
                }
            }
        }
    }

    private double[] getEntireVectorAsDoubles() throws DataSpaceException {
        // this is a tiny vector (we assume - at least the segment size is small) - so iteration is cheap
        double[]  retval = new double[size];
        int i=-1;
        // the following iteration requires a sealed vector, we'll seal it temporarily as we know that this is safe at this point
        boolean originalSealState = isSealed;
        try {
            isSealed = true;
            IVectorIterator<T> iterator = iterator();
            while (iterator.hasNext()) {
                i++;
                retval[i] = iterator.next().getDoubleValue();
            }
        }
        finally {
            isSealed = originalSealState;
        }
        return retval;
    }

    private double[] getFirstSegmentCopyAsDoubles() throws DataSpaceException {
        if (size==0) {
            return new double[0];
        }

        double[] retval = null;
        IVectorSegment<T> first = segments.get(0);
        first.require();
        try {
            retval= first.copyAsDoubles();
        }
        finally {
            first.release();
        }
        return retval;
    }

    @Override
    public boolean isNumeric() {
        BaseType baseType = getBaseType();
        switch (baseType) {
            case DOUBLE:
            case FLOAT:
            case LONG:
            case INTEGER:
            case SHORT:
            case BYTE:
                return true;
            default:
                return false;
        }
    }


    private Quantiles estimateQuantilesOnRestOfSegments(Quantiles firstSegmentQuantiles) throws DataSpaceException {
        int segNum = -1;
        firstSegmentQuantiles.startEstimation(segmentSize);
        for (IVectorSegment<T> segment: segments) {
            segNum++;
            if (segNum > 0) {
                // do the incremental estimate only from the second segment and on
                segment.require();
                try {
                    segment.estimateQuantilesOnRestOfSegments(firstSegmentQuantiles);
                }
                catch (Exception e) {
                    throw new DataSpaceException("Exception while estimating quantiles: "+e);
                }
                finally {
                    segment.release();
                }
            }
        }
        firstSegmentQuantiles.finishEstimation();
        return firstSegmentQuantiles;
    }


    private IDescriptiveStats<T> aggregateSegmentDescriptiveStats() throws DataSpaceException {
        int i=0;
        AbstractSegmentStats<T> retval = null;
        for (AbstractSegmentStats<T> stats: segmentStats) {
            if (i==0) {
                try {
                    retval = stats.clone();
                }
                catch (CloneNotSupportedException e) {
                    throw new DataSpaceException("Exception while cloning stats: "+e,e);
                }
            }
            else {
                retval.add(stats);
            }
            i++;
        }
        return retval;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public T get(int index) throws ArrayIndexOutOfBoundsException, DataSpaceException {
        requireSealedStatus(true);
        // validate index value is OK
        if (index<0 || index>=size()) {
            throw new DataSpaceException("The index "+index+" is out of range for a vector of size "+size());
        }

        // figure out what segment it is in
        int segmentNo = segmentNo(index);

        // require the segment
        IVectorSegment segment = getSegment(segmentNo);
        segment.require();
        T retval = null;
        try {
            retval = get(segment, segmentIndex(index));
        }
        finally {
            segment.release();
        }
        return retval;
    }


    /**
     * Computes the index within a segment.
     * being final - the compiler might inline it.
     * @param vectorIndex
     * @return
     */
    protected final int segmentIndex(int vectorIndex) {
        return vectorIndex % segmentSize;
    }

    /**
     * Computes the segment number
     * being final - the compiler might inline it.
     * @param vectorIndex
     * @return
     */
    public final int segmentNo(int vectorIndex) {
        return vectorIndex/segmentSize;
    }

    /**
     * Creates a new factor value vector based on the unique values of the column and names the factor for it as instructed
     *
     * @param factorName
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    public void factor(String factorName) throws DataSpaceException {
        this.factorName = factorName;
    }

    @Override
    public void factor(String factorName, IFactor factor) throws DataSpaceException {
        this.factorName = factorName;
        this.factor = factor;
    }

    public void factor(Factor factor) throws DataSpaceException {
        this.factorName = factor.getName();
        this.factor = factor;
    }

    @Override
    public boolean isFactor() {
        return factorName!= null;
    }

    @Override
    public IFactor getFactor() throws DataSpaceException {
        if (factorName==null) {
            return null;
        }
        if (factor == null) {
            factor = makeFactor();
        }
        return factor;
    }

    private Factor makeFactor() throws DataSpaceException {
        List<T> values = getSortedUniqueValues();
        return new Factor<T>(factorName, values);
    }

    @Override
    public IBitMap select(IQueryTerm<T> query, IQueryParameterList parameters, IQueryState queryState) throws DataSpaceException {
        // concurrency: probably no need to get a read lock here
        return query.apply(parameters, this, queryState);
    }

    /**
     * Given a bit map of the same length as the vector, produces a vector matching only the true entries in the bit map
     *
     * @param bitmap
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    public IVector<T> subset(IBitMap bitmap) throws DataSpaceException {
        requireSealedStatus(true);
        AbstractVector<T> retval = createNew();
        retval.setPersistenceType(getDataSpace().getDefaultPersistenceType());
        IVectorIterator<T> iterator = iterator(bitmap);
        while (iterator.hasNext()) {
            retval.append(iterator.next());
        }
        return retval.seal();
    }

    @Override
    public IVector<T> subset(IQueryTerm<T> query, IQueryParameterList params, IQueryState queryState) throws DataSpaceException {
        return subset(query.apply(params,this,queryState));
    }

    @Override
    public List<T> asList() throws DataSpaceException {
        return select(null);
    }

    @Override
    public List<T> select(IBitMap selection) throws DataSpaceException {
        requireSealedStatus(true);
        ArrayList<T> retval = new ArrayList<T>();

        List<Integer> positions = null;
        if (selection!=null) {
            positions = selection.getPositions();
        }
        int scanStart = 0;

        for (IVectorSegment<T> segment: segments) {
            // check if the segment is needed
            Integer from = null;
            Integer to = null;
            boolean segmentNeeded = (selection==null); // if no selection then all segments needed
            if (positions != null) {
                int firstPosition = segment.getSegmentNumber()*segmentSize;
                int lastPosition = firstPosition + segment.size()-1;
                for (int i=scanStart; i<positions.size(); i++) {
                    int p = positions.get(i);
                    if (p>lastPosition) {
                        // we're at the next segment now
                        break;
                    }
                    scanStart++;
                    if (p>=firstPosition && p<=lastPosition) {
                        segmentNeeded = true;
                        if (from==null) {
                            from = i;
                        }
                        to = i;
                    }
                }
            }

            if (! segmentNeeded) {
                continue;
            }

            segment.require();
            try {
                segment.appendValues(retval, positions, from, to);
            }
            finally {
                segment.release();
            }
        }

        return retval;
    }

    @Override
    public long sizeInBytes() throws DataSpaceException {
//        requireSealedStatus(true); // not known before it's sealed...
//        long total = 0L;
//        for (IVectorSegment segment: segments) {
//            total += segment.sizeInBytes();
//        }
//        return total;
        return 0L; // we do not report a size what really matters is the size of backing arrays and they report separately
    }

    @Override
    public DataSpace getDataSpace() {
        return dataSpace;
    }

    public boolean supportsResolution() {
        return false;
    }

    /**
     * Creates a unique list of the vector values and sorts it
     *
     * @return the sorted unique list of values
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    @SuppressWarnings("unchecked") // for some reason, the compiler is being pesky about this. It's complaining about the sort, and I am unclear what I am doing wrong.
    public List<T> getSortedUniqueValues(int maxUnique) throws DataSpaceException {
        requireSealedStatus(true);
        HashSet<T> set = new HashSet<T>();

        for (IVectorSegment<T> segment: segments) {
            segment.require();
            try {
                if (supportsResolution()) {
                    set.addAll(segment.getUniqueValues(resolution));
                }
                else {
                    set.addAll(segment.getUniqueValues());
                }
                int size = set.size();
                if (size > maxUnique) {
                    throw new DataSpaceException("Max unique values exceeded. Max="+maxUnique+" reached "+size);
                }
            }
            finally {
                segment.release();
            }
        }

        if (supportsResolution()) {
            set = rebuildWithResolution(set);
        }

        ArrayList<T> retval = new ArrayList<T>(set);
        Collections.sort(retval);
        return retval;
    }

    @Override
    public List<T> getSortedUniqueValues() throws DataSpaceException {
        return getSortedUniqueValues(DEFAULT_MAX_UNIQUE_VALUES);
    }

    @Override
    public List<T> getSortedUniqueValues(IBitMap filter) throws DataSpaceException {
        // naive implementation
        return filter==null ? getSortedUniqueValues() : subset(filter).getSortedUniqueValues();
    }

    @Override
    public int countUnique(IBitMap filter) throws DataSpaceException {
        return getSortedUniqueValues(filter).size();
    }

    /**
     * Rebuilds a unique set using the resolution
     * @param set
     * @return
     */
    private HashSet<T> rebuildWithResolution(HashSet<T> set) {
        HashSet<Long> roundedSet = new HashSet<Long>();
        for (T value: set) {
            roundedSet.add(roundValue(value));
        }
        HashSet<T> retval = new HashSet<T>();
        for (long rounded: roundedSet) {
            retval.add(unRound(rounded));
        }
        return retval;
    }

    /**
     * Transforms the rounded value back to the rough original value using the resolution
     * @param rounded
     * @return
     */
    protected T unRound(long rounded) {
        double restored = resolution*rounded;
        return makeT(restored);
    }

    /**
     * Converts the value to a rounded long value using the resolution
     * @param value
     * @return
     */
    protected Long roundValue(T value) {
        try {
            return Math.round(value.getDoubleValue()/resolution);
        }
        catch (DataSpaceException e) {
            return null;
        }
    }

    public SegmentIterator segmentIterator() {
        return new SegmentIterator();
    }

    @Override
    public IVectorIterator<T> iterator() throws DataSpaceException {
        requireSealedStatus(true);
        return new SimpleVectorIterator<T>(this);
    }

    @Override
    public IVectorIterator<T> iterator(IBitMap result) {
        return new BitmapVectorIterator<T>(this, result);
    }

    /**
     * Creates a new copy of this vector in the target data space, which may be different than the one associated
     * with the vector.
     *
     * @param dataSpace
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException
     *
     */
    @Override
    public IVector<T> copyTo(DataSpace dataSpace) throws DataSpaceException {
        AbstractVector<T> copy = createNew();
        copy.dataSpace = dataSpace;
        copy.setPersistenceType(getPersistenceType());
        copy.segmentSize = dataSpace.getSegmentSize();
        copy.factorName = factorName;
        copy.factor = factor;

        IVectorIterator<T> iterator = iterator();
        copy.append(this);
        copy.seal();

        return copy;
    }

    // Support for resolution ------------------------------------------------------------------------------------------



    /**
     * Is vector resolution automatically determined (by default true)
     *
     * @return true if the resolution was determined automatically
     */
    protected boolean isAutoResolution() {
        return autoResolution;
    }

    /**
     * Gets the vector's resolution
     *
     * @return
     */
    protected double getResolution() {
        return resolution;
    }

    /**
     * sets the vector's resolution. Can only be done before it is sealed
     *
     * @param resolution
     */
    protected void setResolution(double resolution) throws DataSpaceException {
        requireSealedStatus(false);
        autoResolution = false;
        this.resolution = resolution;
    }

    /**
     * Based on the resolution, is there more than one distinct value in the vector?
     *
     * @return
     */
    @Override
    public boolean hasMoreThanOneValue() throws DataSpaceException {
        requireSealedStatus(true);
        Object maxValue = getStats().getDescriptiveStats().getMax();
        Object minValue = getStats().getDescriptiveStats().getMin();

        switch(getBaseType()) {
            case DOUBLE:
            case FLOAT:
                return ((Double)maxValue - (Double)minValue)>resolution;
            default:
                return ! minValue.equals(maxValue);

        }
    }

    protected void conditionallyCalculateResolution() throws DataSpaceException {
        if (autoResolution) {
            // no resolution set by user
            double max = ((Double) getStats().getDescriptiveStats().getMax());
            double min = ((Double) getStats().getDescriptiveStats().getMin());
            double impliedGrain = (max-min)/getDataSpace().getDefaultResolutionRangeDivisor();
            double base10 = Math.log(10);
            long scale = Math.round(Math.log(impliedGrain) / base10);
            resolution = Math.exp(scale * base10);  // get close to a decimal resolution, not some weird number
        }
    }

    public void markAllSegmentsSwappedOut() throws DataSpaceException {
        for (IVectorSegment<T> segment: segments) {
            ((AbstractVectorSegment)segment).initAfterLoadFromDisk();
        }
    }

    public void setDataSpace(DataSpace dataSpace) {
        this.dataSpace = dataSpace;
    }

    /**
     * Causes the vector to swap out all of its segments and to become inoperational
     */
    public void close() {
        closeHelper.close();
    }


    /**
     * Called as part of restoring the vector from disk
     */
    public void initCloseHelper() {
        if (closeHelper==null) {
            closeHelper = new CloseHelper();
        }
    }

    public boolean isClosed() {
        return closeHelper.isClosed();
    }

    public boolean isSealed() {
        return isSealed;
    }

    @Override
    public boolean isVector() {
        return true;
    }

    protected void throwIncompatibleSegmentError(IVectorSegment sourceSegment) throws DataSpaceException {
        throw new DataSpaceException("Incompatible segment type: "+sourceSegment.getClass().getSimpleName());
    }

    // =================================================================================================================

    /**
     * Used in iteration over the vector by query terms
     */
    public class SegmentInfo {
        private IVectorSegment<T> segment;
        private ISegmentStats stats;
        private int segmentNumber;

        protected SegmentInfo(IVectorSegment<T> segment, ISegmentStats stats, int segmentNumber) {
            this.segment = segment;
            this.stats = stats;
            this.segmentNumber = segmentNumber;
        }

        public IVectorSegment<T> getSegment() {
            return segment;
        }

        public int getSegmentNumber() {
            return segmentNumber;
        }

        public ISegmentStats getStats() {
            return stats;
        }

        public boolean isBackingArrayLoaded() {
            return this.segment.isBackingArrayLoaded();
        }
    }

    public class SegmentIterator implements Iterator<SegmentInfo> {
        private int nextSegment;

        protected SegmentIterator() {
            nextSegment = 0;
        }

        /**
         * Returns <tt>true</tt> if the iteration has more elements. (In other
         * words, returns <tt>true</tt> if <tt>next</tt> would return an element
         * rather than throwing an exception.)
         *
         * @return <tt>true</tt> if the iterator has more elements.
         */
        @Override
        public boolean hasNext() {
            return nextSegment < segments.size();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration.
         * @throws java.util.NoSuchElementException
         *          iteration has no more elements.
         */
        @Override
        public SegmentInfo next() {
            int i = nextSegment;
            nextSegment++;
            return new SegmentInfo(segments.get(i), segmentStats.get(i), i);
        }

        /**
         * Removes from the underlying collection the last element returned by the
         * iterator (optional operation).  This method can be called only once per
         * call to <tt>next</tt>.  The behavior of an iterator is unspecified if
         * the underlying collection is modified while the iteration is in
         * progress in any way other than by calling this method.
         *
         * @throws UnsupportedOperationException if the <tt>remove</tt>
         *                                       operation is not supported by this Iterator.
         * @throws IllegalStateException         if the <tt>next</tt> method has not
         *                                       yet been called, or the <tt>remove</tt> method has already
         *                                       been called after the last call to the <tt>next</tt>
         *                                       method.
         */
        @Override
        public void remove() {
            // Do nothing. Immutable
        }
    }

    @Override
    public void setMinimumPersistenceType(PersistenceType persistenceType) {
        minimumPersistenceType = persistenceType;
    }

    @Override
    public void setNameSpace(INameSpace nameSpace) throws DataSpaceException {
        if (minimumPersistenceType!=null && nameSpace.getDefaultPersistenceType().compareTo(minimumPersistenceType)<0) {
            throw new DataSpaceException("Cannot assign this vector to the namespace "+nameSpace.getName()+". The name space persistence type ("+nameSpace.getDefaultPersistenceType()+") is less than the minimum for this vector ("+minimumPersistenceType+"). This is probably due to membership in a data frame with this persistence type");
        }
        super.setNameSpace(nameSpace);
    }

    public void setPersistenceTypeOnAllSegments(PersistenceType type) {
        super.setPersistenceType(type);
        for (IVectorSegment<T> segment: segments) {
            segment.setPersistenceType(type);
        }
    }

    @Override
    public boolean isSorted() {
        Object value = metaData.get(IS_SORTED);
        return (value != null && value instanceof Boolean && (Boolean)value);
    }

    @Override
    public AbstractVector<T> setSorted(boolean isSorted) throws DataSpaceException {
        requireSealedStatus(false);
        metaData.put(IS_SORTED, isSorted);
        return this;
    }

    @Override
    public IVector getVector() throws DataSpaceException {
        requireSealedStatus(true);
        return this;
    }

    @Override
    public long getFirsSegmentSizeInBytes() throws DataSpaceException {
        requireSealedStatus(true);
        if (segments.size()==0) {
            throw new DataSpaceException("No segments in this vector...");
        }
        return segments.get(0).sizeInBytes();
    }

    @Override
    public void forEach(Consumer<? super T> action) throws DataSpaceException {
        // iterate over segments
        for (IVectorSegment<T> segment: segments) {
            segment.require();

            try {
                // iterate over the segment, applying action to each element
                segment.forEach(action);
            }
            catch (Throwable ex) {
                // log exception
                return;
            }
            finally {
                segment.release();
            }
        }
        throw new NotImplementedException("IVector.forEach(action)");
        //FIXME implement this
    }

    @Override
    public void forEach(IBitMap bitmap, Consumer<? super T> action) {
        throw new NotImplementedException("IVector.forEach(action)");
        //FIXME implement this

    }
}
