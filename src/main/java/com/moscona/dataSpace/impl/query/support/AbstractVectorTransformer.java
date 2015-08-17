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

package com.moscona.dataSpace.impl.query.support;

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.impl.AbstractVector;

/**
 * Created: 12/30/10 4:27 PM
 * By: Arnon Moscona
 */
public abstract class AbstractVectorTransformer<T extends IScalar,  TOutput extends IDataElement> implements ITransformer<IVector<T>,TOutput> {

    protected double resolution;
    protected int vectorSegmentSize;
    /**
     * if present than only true entries will be considered (directly supported only by some implementations -
     * others can use the parameters of quickTransform and bulkTransform)
     */
    protected IBitMap selection=null;

    protected abstract void initializeTransformation(IVector<T> vector) throws DataSpaceException;

    protected abstract boolean quickTransform(ISegmentStats stats, int segmentNumber,
                                              boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                              boolean useResolution, double resolution,
                                              IQueryState queryState) throws DataSpaceException;

    protected abstract boolean canProcessInBulk();

    protected abstract void bulkTransform(AbstractVector.SegmentInfo segmentInfo,
                                          boolean useFiltering, int nextSelectedIndex, IPositionIterator positionIterator,
                                          boolean useResolution, double resolution,
                                          IQueryState queryState) throws DataSpaceException;

    protected abstract void transformOne(T element, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException;

    protected abstract TOutput finishTransformation() throws DataSpaceException;

    /**
     * Performs a one-pass evaluation using a pattern similar to the AbstractQueryTerm, but different as here there
     * is no progressive bitmap, and in fact the whole result is controlled by the subclass
     * @param vector
     * @param selection
     * @param queryState
     * @return
     * @throws DataSpaceException
     */
    @Override
    @SuppressWarnings({"unchecked"})
    public TOutput transform(IVector<T> vector, IBitMap selection, IQueryState queryState) throws DataSpaceException {
        setSelection(selection);
        IBitMap useSelection = selection;
        if (abstractTransformerShouldIgnoreSelection()) {
            useSelection = null;
        }

        if(!((IVector) vector).isReadyToQuery()) {
            throw new DataSpaceException("The vector you are trying to query is not ready for query (probably not sealed)");
        }

        queryState.markVectorEvaluationStart();
        boolean useResolution = false;
        double resolution = 0.00001; // arbitrary - will get overwritten if we need resolution support
        if (INumericResolutionSupport.class.isAssignableFrom(vector.getClass())) {
            INumericResolutionSupport support = (INumericResolutionSupport)vector;
            useResolution = true;
            resolution = support.getResolution();
        }

        vectorSegmentSize = ((AbstractVector<T>) vector).getSegmentSize();
        initializeTransformation(vector);
        IPositionIterator selectionIterator = useSelection!=null ? selection.getPositionIterator() : null;
        if (useSelection!=null && !selectionIterator.hasNext()) {
            return null; // nothing selected
        }
        int nextSelected = getNextSelected(selectionIterator, vector);
        int currentIndex = 0; // used both for segment rejection and for row by row iteration when bulk evaluation is not supported

        try {
            validateNotNull(vector,"vector");
            validateNotNull(queryState,"query state");

            // iterate over the segments
            AbstractVector.SegmentIterator iterator = ((AbstractVector)vector).segmentIterator();
            int segmentNumber = 0;

            while(iterator.hasNext()) {
                AbstractVector.SegmentInfo segmentInfo = iterator.next();
                queryState.markSegmentEvaluationStart(segmentNumber);
                segmentNumber = segmentInfo.getSegmentNumber();
                // do we even need to evaluate this segment based on the selection?
                int firstIndex = segmentNumber * ((AbstractVector) vector).getSegmentSize();
                int lastIndex = firstIndex + segmentInfo.getStats().getCount() -1;
                boolean wasQuickEvaluated = (useSelection!=null && ! (nextSelected>=firstIndex && nextSelected<=lastIndex));  // there is a selection but the next index is not in the current segment

                if (!wasQuickEvaluated) {
                    // quick-evaluate segment
                    wasQuickEvaluated = quickTransform(segmentInfo.getStats(), segmentNumber,
                            useSelection!=null, nextSelected, selectionIterator,
                            useResolution, resolution, queryState);
                }
                if (wasQuickEvaluated) {
                    queryState.incQuickSegmentEvals(wasQuickEvaluated);


                    if (useSelection != null) {
                        if (! selectionIterator.hasNext()) {
                            break; // if we exhausted the selection then we're done and we need to break the loop
                        }
                        // fast-forward the iterator past the last index of the current segment
                        int infinity = vector.size();
                        nextSelected = selectionIterator.fastForwardPast(lastIndex, infinity);
                        if (nextSelected >= infinity) {
                            break; // if we exhausted the selection then we're done and we need to break the loop
                        }
                    }
                    continue; // we were able to update the results just by looking at the stats
                }

                // now we have to look at the actual data, we need to ensure it's there for the duration of the processing
                segmentInfo.getSegment().require();
                try {
                    // decide on type of traversal
                    if (canProcessInBulk()) {
                        // supports bulk segment evaluation: evaluate the segment in bulk
                        queryState.incBulkSegmentEvals();
                        // at this point we also know that if there is a selection then the next index is in the segment
                        bulkTransform(segmentInfo,
                                useSelection!=null, nextSelected, selectionIterator,
                                useResolution, resolution, queryState);
                        currentIndex += segmentInfo.getStats().getCount(); // fast-forward the current index to the beginning of the next segment
                        if (useSelection != null) {
                            nextSelected = selectionIterator.lastReturnedValue();  // the bulk transform likely used the iterator (side effect)
                        }
                    }
                    else {
                        // does not support bulk segment evaluation: iterate element by element
                        queryState.incSlowSegmentEvals();
                        IVectorSegment<T> segment = (IVectorSegment<T>)segmentInfo.getSegment();  // IMPORTANT this creates an unchecked warning. not clear why
                        ISegmentIterator<T> segmentIterator = segment.iterator();
                        while(segmentIterator.hasNext()) {
                            T element = segmentIterator.next();
                            boolean shouldElementBeEvaluated = useSelection==null || currentIndex == nextSelected;
                            if (shouldElementBeEvaluated) {
                                transformOne(element, useResolution, resolution, queryState);
                                nextSelected = getNextSelected(selectionIterator, vector); // could get an index past the vector end
                            }
                            currentIndex++;
                        }
                    }

                    queryState.incCompletedSegments(segmentNumber, 0);
                }
                catch (DataSpaceException e) {
                    queryState.signalSegmentException(e, segmentNumber);
                    throw e;
                }
                catch (Exception e) {
                    DataSpaceException dataSpaceException = new DataSpaceException("Exception while performing transformation: " + e, e);
                    queryState.signalSegmentException(dataSpaceException, segmentNumber);
                    throw dataSpaceException;
                }
                finally {
                    segmentInfo.getSegment().release();
                }
            }

            queryState.markCompletedVectorEvaluation(0);
        }
        catch (DataSpaceException e) {
            queryState.signalVectorException(e);
            throw e;
        }

        return finishTransformation();
    }

    protected void setSelection(IBitMap selection) throws DataSpaceException {
        this.selection = selection; // for the benefit of those implementations that wish to use the selection directly
    }

    protected boolean abstractTransformerShouldIgnoreSelection() {
        return false;
    }

    private int getNextSelected(IPositionIterator selectionIterator, IVector vector) throws DataSpaceException {
        if (selectionIterator==null) {
            return -1;
        }
        if (selectionIterator.hasNext()) {
            return selectionIterator.next();
        }
        else {
            return vector.size(); // past the end
        }
    }

    @Override
    public TOutput transform(IVector<T> vector, IQueryState queryState) throws DataSpaceException {
        return transform(vector, null, queryState);
    }

    protected void validateNotNull(Object param, String name) throws DataSpaceException {
        if(param==null) {
            throw new DataSpaceException("Error: "+name+" may not be null");
        }
    }

    protected long round(double value) {
        return Math.round(value/resolution);
    }

    protected int countSelectedElementsInSegment(int segmentNumber, int thisSegmentSize, int nextSelectedIndex, IPositionIterator positionIterator) throws DataSpaceException {
        int firstIndex = vectorSegmentSize * segmentNumber;
        int lastIndex = firstIndex + thisSegmentSize;
        int counter = 0;
        int testValue = nextSelectedIndex;
        while (testValue>=firstIndex && testValue<=lastIndex) {
            counter++;
            if (positionIterator.hasNext()) {
                testValue = positionIterator.next();
            }
            else {
                return counter;
            }
        }
        return counter;
    }
}
