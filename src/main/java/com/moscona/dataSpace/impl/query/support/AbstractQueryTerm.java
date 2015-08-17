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
import com.moscona.dataSpace.util.CompressedBitMap;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created: 12/16/10 11:59 AM
 * By: Arnon Moscona
 */
public abstract class AbstractQueryTerm<T extends IScalar> implements IQueryTerm<T> {
    protected IQueryParameterList params = null;
    protected IVector.BaseType baseType = null;

    /**
     * Creates an empty parameter list that is appropriate for this term.
     *
     * @return
     */
    @Override
    public IQueryParameterList createParameterList(IVector.BaseType baseType) throws DataSpaceException {
        QueryParameterList retval = new QueryParameterList();
        populateEmptyParameterList(retval, baseType);
        return retval;
    }

    /**
     * Called during query parameter list construction to have the subclass populate it with its parameters
     * @param retval
     */
    protected abstract void populateEmptyParameterList(QueryParameterList retval, IVector.BaseType baseType) throws DataSpaceException;

    /**
     * Sets the parameters for this term before starting any evaluation. Do not perform any evaluation except parsing
     * the parameters, storing them (you won't get them again), and validating the. If you throw an exception here
     * then the query evaluation will be aborted.
     * @param params
     * @param vector
     * @throws DataSpaceException
     */
    protected abstract void setParameters(IQueryParameterList params, IVector<T> vector) throws DataSpaceException;

    /**
     * The subclass is required to tell us whether it can directly access the backing array for "bulk" evaluation or
     * that we need to iterate on its behalf. Bulk access is far faster than iteration and its preferable. If you choose
     * bulk access, you must ensure that you never modify the segment data.
     * If you choose bulk you do not have to implement the match(element) method.
     * If you do not choose bulk you do not need to implement the bulkMatch method
     * @return
     */
    protected abstract boolean canProcessInBulk();

    /**
     * An opportunity for the term to be evaluated based on the segment stats alone without looking at any of the
     * concrete data
     * @param stats
     * @param segmentNumber
     * @param useResolution true if you should use the resolution parameter to determine value equivalence
     * @param resolution if(useResolution) then a.equals(b) iff (abs(a-b) < resolution)
     * @param queryState
     * @return true or false if there is a uniform result to the whole segment, null if unable to determine
     */
    protected abstract Boolean quickMatch(ISegmentStats stats, int segmentNumber, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException;

    /**
     * Evaluates one data element at a time, returning true if it passed the match and false otherwise. This is much
     * less efficient than bulk matching, but is easier to implement and safer (immutability is guaranteed)
     * @param element
     * @param useResolution true if you should use the resolution parameter to determine value equivalence
     * @param resolution if(useResolution) then a.equals(b) iff (abs(a-b) < resolution)
     * @param queryState
     * @return
     */
    protected abstract boolean match(T element, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException;


    /**
     * Evaluates the segment as a whole using direct access to its backing array
     * @param segmentInfo
     * @param progressiveResult
     * @param useResolution true if you should use the resolution parameter to determine value equivalence
     * @param resolution if(useResolution) then a.equals(b) iff (abs(a-b) < resolution)
     * @param queryState
     * @throws DataSpaceException
     */
    protected abstract void bulkMatch(AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, boolean useResolution, double resolution, IQueryState queryState) throws DataSpaceException;


    @Override
    public IBitMap apply(IQueryParameterList params, IVector<T> vector, IQueryState queryState) throws DataSpaceException {
        return apply(params,vector,queryState,null);
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public IBitMap apply(IQueryParameterList params, IVector<T> vector, IQueryState queryState, IBitMap intersectWith) throws DataSpaceException {
        // concurrency HOLD (fix before release)  obtain vector read lock (probably not needed see #IT-477)
        // validate the parameters
        if(!vector.isReadyToQuery()) {
            throw new DataSpaceException("The vector you are trying to query is not ready for query (probably not sealed)");
        }
        HashSet<Integer> segmentSkipList = makeSegmentSkipList((AbstractVector<T>) vector, intersectWith);
        IBitMap progressiveResult = new CompressedBitMap();
        queryState.markVectorEvaluationStart();
        boolean useResolution = false;
        double resolution = 0.00001; // arbitrary - will get overwritten if we need resolution support
        if (INumericResolutionSupport.class.isAssignableFrom(vector.getClass())) {
            INumericResolutionSupport support = (INumericResolutionSupport)vector;
            useResolution = true;
            resolution = support.getResolution();
        }


        try {
            validateNotNull(params,"params");
            validateNotNull(vector,"vector");
            validateNotNull(queryState,"query state");
            setParameters(params, vector);

            // iterate over the segments
            AbstractVector.SegmentIterator iterator = ((AbstractVector)vector).segmentIterator();
            int segmentNumber = 0;

            while(iterator.hasNext()) {
                AbstractVector.SegmentInfo segmentInfo = iterator.next();
                queryState.markSegmentEvaluationStart(segmentNumber);
                segmentNumber = segmentInfo.getSegmentNumber();
                if (segmentSkipList.contains(segmentNumber)) {
                    applyToAll(false, segmentInfo, progressiveResult, queryState);
                    queryState.incSkippedSegments();
                    continue;
                }

                // quick-evaluate segment
                Boolean result = quickMatch(segmentInfo.getStats(), segmentNumber, useResolution, resolution, queryState);
                if (result != null) {
                    applyToAll(result, segmentInfo, progressiveResult, queryState);
                    queryState.incQuickSegmentEvals(result);
                    continue; // we were able to update the results just by looking at the stats
                }

                // now we have to look at the actual data, we need to ensure it's there for the duration of the processing
                segmentInfo.getSegment().require();
                try {
                    // decide on type of traversal
                    if (canProcessInBulk()) {
                        // supports bulk segment evaluation: evaluate the segment in bulk
                        queryState.incBulkSegmentEvals();
                        bulkMatch(segmentInfo, progressiveResult, useResolution, resolution, queryState);
                    }
                    else {
                        // does not support bulk segment evaluation: iterate element by element
                        queryState.incSlowSegmentEvals();
                        IVectorSegment<T> segment = (IVectorSegment<T>)segmentInfo.getSegment();  // IMPORTANT this creates an unchecked warning. not clear why
                        ISegmentIterator<T> segmentIterator = segment.iterator();
                        while(segmentIterator.hasNext()) {
                            T element = segmentIterator.next();
                            progressiveResult.add(match(element, useResolution, resolution, queryState));
                        }
                    }
                }
                catch (DataSpaceException e) {
                    queryState.signalSegmentException(e, segmentNumber);
                    throw e;
                }
                finally {
                    segmentInfo.getSegment().release(true);
                }
                queryState.incCompletedSegments(segmentNumber, progressiveResult.cardinality());
            }

            queryState.markCompletedVectorEvaluation(progressiveResult.cardinality());
        }
        catch (DataSpaceException e) {
            queryState.signalVectorException(e);
            throw e;
        }


        // return the result
        if (intersectWith!=null) {
            return progressiveResult.and(intersectWith);
        }
        return progressiveResult;
    }

    private HashSet<Integer> makeSegmentSkipList(AbstractVector<T> vector, IBitMap intersectWith) throws DataSpaceException {
        HashSet<Integer> retval = new HashSet<Integer>();
        if (intersectWith==null) {
            return retval;
        }

        for (int i=0; i<vector.getSegmentCount(); i++) {
            retval.add(i); // skip all segments until we find that they are needed
        }

        IPositionIterator iterator = intersectWith.getPositionIterator();
        while (iterator.hasNext()) {
            int truePosition = iterator.next();
            int segmentNumber = vector.segmentNo(truePosition);
            retval.remove(segmentNumber); // we need this segment
        }

        return retval;
    }

    /**
     * Applies the result to all the elements of the segment
     * @param result
     * @param segmentInfo
     * @param progressiveResult
     * @param queryState
     */
    protected final void applyToAll(boolean result, AbstractVector.SegmentInfo segmentInfo, IBitMap progressiveResult, IQueryState queryState) {
        int size = segmentInfo.getSegment().size();
        for (int i=0;i<size;i++) {
            progressiveResult.add(result);
        }
        queryState.incQuickApplyElements();
    }

    protected void validateNotNull(Object param, String name) throws DataSpaceException {
        if(param==null) {
            throw new DataSpaceException("Error: "+name+" may not be null");
        }
    }

    protected void validateParamNames(IQueryParameterList params, IVector.BaseType baseType) throws DataSpaceException {
        QueryParameterList template = new QueryParameterList();
        populateEmptyParameterList(template, baseType);
        ArrayList<IQueryParameter> templateParams =template.getParams();
        ArrayList<IQueryParameter> testParams = ((QueryParameterList)params).getParams();
        for (int i=0; i<templateParams.size(); i++) {
            String expected = templateParams.get(i).getName();
            String actual = testParams.get(i).getName();
            if (! expected.equals(actual)) {
                throw new DataSpaceException("Parameter "+i+" does not match. Expected \""+expected+"\" but got \""+actual+"\"");
            }
        }
    }

    protected void throwIncompatibleException(IVector.BaseType baseType) throws DataSpaceException {
        throw new DataSpaceException("The base type "+baseType+" is incompatible with "+this.getClass().getName());
    }

    protected void throwIncompatibleParameterException(String param, IVector.BaseType vectorBaseType, IQueryParameter.ParameterType paramBaseType) throws DataSpaceException {
        throw new DataSpaceException("The parameter base type "+paramBaseType+" for parameter \""+param+"\" is incompatible with the vector base type of "+vectorBaseType+" in " +this.getClass().getName());
    }

    protected boolean toBoolean(String value) throws DataSpaceException {
        String v = value.toLowerCase().trim();
        if (v.equals("true") || v.equals("1") || v.equals("yes") || v.equals("y")) {
            return true;
        }
        else if (v.equals("false") || v.equals("0") || v.equals("no") || v.equals("n")) {
            return false;
        }
        throw new DataSpaceException("The String value \""+value+"\" could not be converted to a boolean");
    }

    /**
     * use as last resort calculation when supporting resolutions and all other evaluations failed (i.e. make the call rare)
     * @param a
     * @param b
     * @param resolution
     * @return
     */
    protected final boolean equals(double a, double  b, double resolution) {
        return Math.abs(a-b)<=resolution;
    }

    @SuppressWarnings({"FloatingPointEquality"})
    protected final boolean equals(double  a, double b, double resolution, boolean useResolution) {
        return useResolution ? Math.abs(a-b)<=resolution : a==b;
    }

    protected Boolean quickEval(double from, double to, double min, double max, boolean leftClosed, boolean rightClosed, boolean useResolution, double resolution) {

        // if everything looks equal and there is only one value based on resolution then it's all true
        if (useResolution &&
                equals(min,max,resolution) &&
                equals(from,to,resolution) &&
                equals(from,min,resolution)) {
            return true;
        }

        // if max is below the from or the min is above the to then it's false for the whole thing
        boolean maxIsSmall = leftClosed ? (max < from) : ((max <= from) || (useResolution && equals(max,from,resolution)));
        boolean minIsLarge = rightClosed ? (min > to) : ((min >= to) || (useResolution && equals(min,to,resolution)));
        if (maxIsSmall || minIsLarge) {
            return false;
        }

        return null;
    }

    protected Boolean quickEval(long from, long to, long min, long max, boolean leftClosed, boolean rightClosed) {
        if (min==max && from==to && from==min) {
            return true; // there is only one value in the range and in the target range and it's all the same value
        }

        // if max is below the from or the min is above the to then it's false for the whole thing
        boolean maxIsSmall = leftClosed ? (max < from) : (max <= from);
        boolean minIsLarge = rightClosed ? (min > to) : (min >= to);
        if (maxIsSmall || minIsLarge) {
            return false;
        }

        return null;
    }

    protected Boolean quickEval(String value, String min, String max, boolean leftClosed, boolean rightClosed) {
         // if there is only one value in the range and it's the value we're after then it's all true
        if (max.equals(min) && min.equals(value)) {
            return true;
        }

        int minComp = min.compareTo(value);
        int maxComp = max.compareTo(value);

        // if max is below the value or the min is above the value then it's false for the whole thing
        boolean maxIsSmall = rightClosed ? (maxComp < 0) : (maxComp <= 0);
        boolean minIsLarge = leftClosed ? (minComp > 0) : (minComp >= 0);
        if (maxIsSmall || minIsLarge) {
            return false;
        }

        return null;
    }

    protected  Boolean quickEval(boolean value, boolean min, boolean max) {
        if (max!=min) {
            return null;
        }
        return value==min;
    }

    protected void validateNoSetParams(IQueryParameterList params) throws DataSpaceException {
        for (IQueryParameter param: ((QueryParameterList) params).getParams()) {
            if(param.getType() == IQueryParameter.ParameterType.STRING_SET ||
               param.getType() == IQueryParameter.ParameterType.LONG_SET) {
                throw new DataSpaceException("No set parameters allowed in this query. The parameter "+param.getName()+" is a "+param.getType());
            }
        }
    }
}
