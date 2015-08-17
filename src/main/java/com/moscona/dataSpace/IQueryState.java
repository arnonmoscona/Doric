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

/**
 * Created: 12/10/10 11:36 AM
 * By: Arnon Moscona
 * Tracks query state through entire execution over multiple vectors
 */
public interface IQueryState {
    /**
     * Marks the time when a vector evaluation started
     */
    void markVectorEvaluationStart();

    /**
     * Marks the time when a segment evaluation started
     * @param segmentNumber
     */
    void markSegmentEvaluationStart(int segmentNumber);

    /**
     * informs of a segment that was evaluated based on the stats alone
     * @param result the uniform result for this segment
     */
    void incQuickSegmentEvals(boolean result);

    /**
     * informs of a segment that was evaluated using bulk evaluation and direct backing array access
     */
    void incBulkSegmentEvals();

    /**
     * informs of a segment that was evaluated element by element
     */
    void incSlowSegmentEvals();

    /**
     * informs of an exception during segment evaluation
     * @param e the exception
     * @param segmentNumber the segment number that was evaluated
     */
    void signalSegmentException(DataSpaceException e, int segmentNumber);

    /**
     * informs of a successful completion of segment evaluation
     * @param segmentNumber the segment number
     * @param cardinality the cardinality of the progressive result after the segment completed
     */
    void incCompletedSegments(int segmentNumber, int cardinality);

    /**
     * informs of the successful completion of vector evaluation
     * @param cardinality
     */
    void markCompletedVectorEvaluation(int cardinality);

    /**
     * informs of a failed completion of vector evaluation
     * @param e
     */
    void signalVectorException(DataSpaceException e);

    /**
     * informs of a bulk update of a uniform result to a segment
     * (regardless of whether it was done in a base class or a subclass)
     */
    void incQuickApplyElements();

    /**
     * Sets the cumulative result so far, such that terms can use it to possibly skip segments that have been already eliminated
     * @param cumulativeResult
     */
    void setCumulativeResult(IBitMap cumulativeResult);

    void setContextIsIntersectionGroup(boolean contextIsIntersectionGroup);

    void incSkippedSegments();

    int getSkippedSegments();
}
