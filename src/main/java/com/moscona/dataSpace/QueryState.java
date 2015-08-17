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

import java.util.ArrayList;

/**
 * Created: 12/17/10 4:22 PM
 * By: Arnon Moscona
 */
public class QueryState implements IQueryState {
    private long vectorEvaluationStartTs=-1L;
    private long creationTs;
    private long totalTime = 0L;

    private ArrayList<SegmentEvaluation> segmentEvaluations;
    private IBitMap cumulativeResult=null;
    private Throwable exception=null;
    private long timeToException=-1;
    private boolean isComplete=false;
    private SegmentEvaluation currentSegment = null;
    private int quickSegmentEvalCount =0;
    private int bulkSegmentEvalCount =0;
    private int slowSegmentEvalCount =0;
    private int uniformResultQuickApplyCount=0;
    private int segmentNumberWhereExceptionHappened=-1;
    private boolean queryStateBugDetected = false;
    private String queryStateBug = null;
    private boolean inContextOfIntersectionGroup = false;
    private int skippedSegments = 0;

    public QueryState() {
        creationTs = System.currentTimeMillis();
        segmentEvaluations = new ArrayList<SegmentEvaluation>();
    }

    private void bug(String what) {
        queryStateBugDetected = true;
        queryStateBug = what;
    }

    /**
     * Marks the time when a vector evaluation started
     */
    @Override
    public void markVectorEvaluationStart() {
        if (vectorEvaluationStartTs<0) {
            vectorEvaluationStartTs = System.currentTimeMillis();
        }
    }

    /**
     * Marks the time when a segment evaluation started
     *
     * @param segmentNumber
     */
    @Override
    public void markSegmentEvaluationStart(int segmentNumber) {
        currentSegment = new SegmentEvaluation(segmentNumber);
        segmentEvaluations.add(currentSegment);
    }

    /**
     * informs of a segment that was evaluated based on the stats alone
     *
     * @param result the uniform result for this segment
     */
    @Override
    public void incQuickSegmentEvals(boolean result) {
        markVectorEvaluationStart();
        if (currentSegment!=null) {
            currentSegment.setQuickEval(true);
            quickSegmentEvalCount++;
        }
        else {
            bug("incQuickSegmentEvals() called and there's no current segment");
        }
    }

    /**
     * informs of a segment that was evaluated using bulk evaluation and direct backing array access
     */
    @Override
    public void incBulkSegmentEvals() {
        markVectorEvaluationStart();
        if (currentSegment!=null) {
            currentSegment.setBulkEval(true);
            bulkSegmentEvalCount++;
        }
        else {
            bug("incBulkSegmentEvals() called and there's no current segment");
        }
    }

    /**
     * informs of a segment that was evaluated element by element
     */
    @Override
    public void incSlowSegmentEvals() {
        markVectorEvaluationStart();
        if (currentSegment!=null) {
            currentSegment.setSlowEval(true);
            slowSegmentEvalCount++;
        }
        else {
            bug("incSlowSegmentEvals() called and there's no current segment");
        }
    }

    /**
     * informs of an exception during segment evaluation
     *
     * @param e             the exception
     * @param segmentNumber the segment number that was evaluated
     */
    @Override
    public void signalSegmentException(DataSpaceException e, int segmentNumber) {
        markException(e);
        markTotalTime(); // presumably done now
        if (currentSegment!=null) {
            segmentNumberWhereExceptionHappened = currentSegment.segmentNumber;
            currentSegment.setException(e);
        }
        else {
            bug("signalSegmentException() called and there's no current segment");
        }
    }

    private void markException(DataSpaceException e) {
        exception = e;
        timeToException = System.currentTimeMillis() - vectorEvaluationStartTs;
    }

    private void markTotalTime() {
        totalTime = System.currentTimeMillis() - vectorEvaluationStartTs;
    }

    /**
     * informs of a successful completion of segment evaluation
     *
     * @param segmentNumber the segment number
     * @param cardinality   the cardinality of the progressive result after the segment completed
     */
    @Override
    public void incCompletedSegments(int segmentNumber, int cardinality) {
        if (currentSegment!=null) {
            if (segmentNumber==currentSegment.segmentNumber) {
                currentSegment.markComplete(cardinality);
            }
            else {
                bug("incCompletedSegments() called for segment #"+segmentNumber+" but current segment is #"+currentSegment.segmentNumber);
            }
        }
        else {
            bug("incCompletedSegments() called and there's no current segment");
        }
    }

    /**
     * informs of the successful completion of vector evaluation
     *
     * @param cardinality
     */
    @Override
    public void markCompletedVectorEvaluation(int cardinality) {
        isComplete = true;
        if (vectorEvaluationStartTs>0) {
            markTotalTime();
        }
        else {
            bug("markCompletedVectorEvaluation() called before we marked vector evaluation start");
        }
    }

    /**
     * informs of a failed completion of vector evaluation
     *
     * @param e
     */
    @Override
    public void signalVectorException(DataSpaceException e) {
        markException(e);
    }

    /**
     * informs of a bulk update of a uniform result to a segment
     * (regardless of whether it was done in a base class or a subclass)
     */
    @Override
    public void incQuickApplyElements() {
        uniformResultQuickApplyCount++;
        if (currentSegment!=null) {
            currentSegment.setQuickApply(true);
        }
        else {
            bug("incQuickApplyElements() called and there's no current segment");
        }
    }

    @Override
    public void setContextIsIntersectionGroup(boolean contextIsIntersectionGroup) {
        inContextOfIntersectionGroup = contextIsIntersectionGroup;
    }

    @Override
    public void incSkippedSegments() {
        skippedSegments++;
    }

    @Override
    public int getSkippedSegments() {
        return skippedSegments;
    }

    /**
     * Sets the cumulative result so far, such that terms can use it to possibly skip segments that have been already eliminated
     *
     * @param cumulativeResult
     */
    @Override
    public void setCumulativeResult(IBitMap cumulativeResult) {
        this.cumulativeResult = cumulativeResult;
    }

    public int getBulkSegmentEvalCount() {
        return bulkSegmentEvalCount;
    }

    public long getCreationTs() {
        return creationTs;
    }

    public IBitMap getCumulativeResult() {
        return cumulativeResult;
    }

    public Throwable getException() {
        return exception;
    }

    public boolean isInContextOfIntersectionGroup() {
        return inContextOfIntersectionGroup;
    }

    public boolean isComplete() {
        return isComplete;
    }

    public String getQueryStateBug() {
        return queryStateBug;
    }

    public boolean isQueryStateBugDetected() {
        return queryStateBugDetected;
    }

    public int getQuickSegmentEvalCount() {
        return quickSegmentEvalCount;
    }

    public ArrayList<SegmentEvaluation> getSegmentEvaluations() {
        return segmentEvaluations;
    }

    public int getSegmentNumberWhereExceptionHappened() {
        return segmentNumberWhereExceptionHappened;
    }

    public int getSlowSegmentEvalCount() {
        return slowSegmentEvalCount;
    }

    public long getTimeToException() {
        return timeToException;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public int getUniformResultQuickApplyCount() {
        return uniformResultQuickApplyCount;
    }

    public long getVectorEvaluationStartTs() {
        return vectorEvaluationStartTs;
    }

    @SuppressWarnings({"UseOfSystemOutOrSystemErr"})
    public void dump(boolean inFullDetail) {
        System.out.println("total time: "+totalTime);
        System.out.println("bulk evaluations: "+bulkSegmentEvalCount);
        System.out.println("quick evaluations: "+quickSegmentEvalCount);
        System.out.println("skipped segments: "+skippedSegments);
        System.out.println("slow evaluations: "+slowSegmentEvalCount);
        System.out.println("uniform result quick applies: "+uniformResultQuickApplyCount);
        if (inFullDetail) {
            System.out.println("segment evals:");
            for (SegmentEvaluation eval: segmentEvaluations) {
                System.out.println("  Segment eval -----------------------------------------------");
                System.out.println("  segment No: "+eval.getSegmentNumber());
                System.out.println("  total time: "+eval.getTotalTime());
                if (eval.isBulkEval()) {
                    System.out.println("  eval type: bulk");
                }
                else if (eval.isSlowEval()) {
                    System.out.println("  eval type: slow");
                }
                else if (eval.isQuickEval()) {
                    System.out.println("  eval type: quick");
                }
                DataSpaceException ex = eval.getSegmentException();
                if (ex !=null) {
                    System.out.println("Exception in segment eval: "+ex.getClass().getSimpleName()+" "+ex);
                }
            }
        }
    }

    public class SegmentEvaluation {

        private int segmentNumber;
        private long startTs;
        private boolean quickEval=false;
        private boolean bulkEval=false;
        private boolean slowEval=false;
        private boolean quickApplyUniformResult=false;
        private DataSpaceException segmentException=null;
        private long totalTime=-1L;

        public SegmentEvaluation(int segmentNumber) {
            this.segmentNumber = segmentNumber;
            startTs = System.currentTimeMillis();
        }

        public void setQuickEval(boolean b) {
            quickEval = b;
        }

        public void setBulkEval(boolean b) {
            bulkEval = b;
        }

        public void setSlowEval(boolean b) {
            slowEval = b;
        }

        public void setException(DataSpaceException e) {
            segmentException = e;
            markTotalTime();
        }

        private void markTotalTime() {
            totalTime = System.currentTimeMillis()-startTs;
        }

        public void markComplete(int cardinality) {
            markTotalTime();
        }

        public void setQuickApply(boolean b) {
            quickApplyUniformResult = true;
        }

        public boolean isBulkEval() {
            return bulkEval;
        }

        public boolean isQuickApplyUniformResult() {
            return quickApplyUniformResult;
        }

        public boolean isQuickEval() {
            return quickEval;
        }

        public DataSpaceException getSegmentException() {
            return segmentException;
        }

        public int getSegmentNumber() {
            return segmentNumber;
        }

        public boolean isSlowEval() {
            return slowEval;
        }

        public long getStartTs() {
            return startTs;
        }

        public long getTotalTime() {
            return totalTime;
        }
    }
}
