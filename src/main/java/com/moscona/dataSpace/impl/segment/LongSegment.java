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

import com.moscona.dataSpace.*;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.PersistenceType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created: 12/14/10 3:50 PM
 * By: Arnon Moscona
 */
public class LongSegment extends AbstractVectorSegment<Numeric<Long>,Long>{
    private static final long serialVersionUID = -2140235210046776924L;

    public LongSegment(DataSpace dataSpace, PersistenceType persistenceType, int maxSegmentSize) {
        super(dataSpace, persistenceType, maxSegmentSize, new LongSegmentStats(persistenceType));
    }

    LongSegmentBackingArray myBackingArray() {
        return (LongSegmentBackingArray)getBackingArray();
    }

    public void append(long value) throws DataSpaceException {
        // IMPORTANT - we are still mutable here, and so the memory manager is not involved with this segment yet
        myBackingArray().data[size()] = value;
        incSize(1);
        ((LongSegmentStats) stats).add(value);
    }

    /**
     * Used by segment queries that are not deeply integrated with the native segment class. Iteration increases
     * query time for range queries (over direct access to the backing array) by an estimated multiplier of 4-5
     *
     * @return
     */
    @Override
    public ISegmentIterator<Numeric<Long>> iterator() {
        return new BackingArrayIterator();
    }

    @Override
    public long sizeInBytes() {
        return ((long) Long.SIZE)*size();
    }

    @Override
    public ISegmentStats calculateStats() {
        //HOLD (before release) implement LongSegment.calculateStats (finalize stats, calculate quantiles, histogram etc) - note that range (min/max) is already calculated in the add method... These would become more important when we fix #IT-468 and #IT-464 (a better vector quantile estimation)
        return stats;
    }

    @Override
    protected void trimBackingArray() {
        myBackingArray().trim(size());
    }

    @Override
    protected IVectorSegmentBackingArray<Long> createBackingArray() {
        return new LongSegmentBackingArray(getMaxSegmentSize());
    }

    @Override
    public Set<Numeric<Long>> getUniqueValues() {
        HashSet<Numeric<Long>> retval = new HashSet<Numeric<Long>>();
        for (int i=0; i<size(); i++) {
            Numeric<Long> value = new Numeric<Long>(myBackingArray().data[i]);
            retval.add(value);
        }
        return retval;
    }

    @Override
    public double[] copyAsDoubles() {
        long[] data = ((LongSegmentBackingArray) getBackingArray()).data;
        double[] retval = new double[data.length];
        for (int i=0; i<data.length; i++) {
            retval[i] = data[i];
        }
        return retval;
    }

    @Override
    public void estimateQuantilesOnRestOfSegments(Quantiles quantiles) {
        // caller already initialized and is responsible for iterating over segments. Here we just contribute our data
        for (long observation: ((LongSegmentBackingArray) getBackingArray()).data) {
            quantiles.addObservationToQuantileEstimate(observation);
        }
    }

    public class BackingArrayIterator implements ISegmentIterator<Numeric<Long>> {
        private int currentIndex = -1;

        @Override
        public boolean hasNext() {
            return currentIndex < size()-1;
        }

        @Override
        public Numeric<Long> next() {
            if (! hasNext()) {
                return null;
            }

            currentIndex++;
            return new Numeric<Long>(myBackingArray().data[currentIndex]);
        }

        @Override
        public void reset() {
            currentIndex = -1;
        }

        @Override
        public int currentIndex() {
            return currentIndex;
        }
    } 

    @Override
    public void appendValues(ArrayList<Numeric<Long>> retval, List<Integer> positions, Integer from, Integer to) {
        long[] data = ((LongSegmentBackingArray) getBackingArray()).data;
        if (positions==null) {
            for (long datum: data) {
                retval.add(new Numeric<Long>(datum));
            }
        }
        else {
            int base = getSegmentNumber() * getMaxSegmentSize();
            for (int i=from; i<=to; i++) {
                retval.add(new Numeric<Long>(data[positions.get(i)-base]));
            }
        }
    }
}
