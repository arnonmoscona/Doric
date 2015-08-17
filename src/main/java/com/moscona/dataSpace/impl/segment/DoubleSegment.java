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
 * Created: 12/14/10 5:42 PM
 * By: Arnon Moscona
 */
public class DoubleSegment  extends AbstractVectorSegment<Numeric<Double>,Double>{
    private static final long serialVersionUID = -5021617225479250556L;

    public DoubleSegment(DataSpace dataSpace, PersistenceType persistenceType, int maxSegmentSize) {
        super(dataSpace, persistenceType, maxSegmentSize, new DoubleSegmentStats(persistenceType));
    }

    DoubleSegmentBackingArray myBackingArray() {
        return (DoubleSegmentBackingArray)getBackingArray();
    }

    public void append(double value) throws DataSpaceException {
        // IMPORTANT - we are still mutable here, and so the memory manager is not involved with this segment yet
        myBackingArray().data[size()] = value;
        incSize(1);
        ((DoubleSegmentStats) stats).add(value);
    }

    /**
     * Used by segment queries that are not deeply integrated with the native segment class. Iteration increases
     * query time for range queries (over direct access to the backing array) by an estimated multiplier of 4-5
     *
     * @return
     */
    @Override
    public ISegmentIterator<Numeric<Double>> iterator() {
        return new BackingArrayIterator();
    }

    @Override
    public long sizeInBytes() {
        return ((long) Double.SIZE)*size();
    }

    @Override
    public ISegmentStats calculateStats() {
        //HOLD (before release) implement DoubleSegment.calculateStats (finalize stats, calculate quantiles, histogram etc)  - note that range (min/max) is already calculated in the add method... These would become more important when we fix #IT-468 and #IT-464 (a better vector quantile estimation)
        return stats;
    }

    @Override
    protected void trimBackingArray() {
        myBackingArray().trim(size());
    }

    @Override
    protected IVectorSegmentBackingArray<Double> createBackingArray() {
        return new DoubleSegmentBackingArray(getMaxSegmentSize());
    }

    @Override
    public Set<Numeric<Double>> getUniqueValues() {
        HashSet<Numeric<Double>> retval = new HashSet<Numeric<Double>>();
        for (int i=0; i<size(); i++) {
            Numeric<Double> value = new Numeric<Double>(myBackingArray().data[i]);
            retval.add(value);
        }
        return retval;
    }

    public class BackingArrayIterator implements ISegmentIterator<Numeric<Double>> {
        private int currentIndex = -1;

        @Override
        public boolean hasNext() {
            return currentIndex < size()-1;
        }

        @Override
        public Numeric<Double> next() {
            if (! hasNext()) {
                return null;
            }

            currentIndex++;
            return new Numeric<Double>(myBackingArray().data[currentIndex]);
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
    public Set<? extends Numeric<Double>> getUniqueValues(double resolution) {
        HashSet<Long> roundedSet = new HashSet<Long>();
        HashSet<Numeric<Double>> retval = new HashSet<Numeric<Double>>();
        // create a unique set of rounded numbers
        for (int i=0; i<size(); i++) {
            double value = myBackingArray().data[i];
            roundedSet.add(Math.round(value/resolution));
        }
        // convert the rounded numbers to estimated originals
        for (long rounded: roundedSet) {
            Numeric<Double> value = new Numeric<Double>(resolution*rounded);
            retval.add(value);
        }
        return retval;
    }

    @Override
    public double[] copyAsDoubles() {
        double[] data = ((DoubleSegmentBackingArray) getBackingArray()).data;
        double[] retval = new double[data.length];
        for (int i=0; i<data.length; i++) {
            retval[i] = data[i];
        }
        return retval;
    }

    @Override
    public void estimateQuantilesOnRestOfSegments(Quantiles quantiles) {
        // caller already initialized and is responsible for iterating over segments. Here we just contribute our data
        for (double observation: ((DoubleSegmentBackingArray) getBackingArray()).data) {
            quantiles.addObservationToQuantileEstimate(observation);
        }
    }  

    @Override
    public void appendValues(ArrayList<Numeric<Double>> retval, List<Integer> positions, Integer from, Integer to) {
        double[] data = ((DoubleSegmentBackingArray) getBackingArray()).data;
        if (positions==null) {
            for (double datum: data) {
                retval.add(new Numeric<Double>(datum));
            }
        }
        else {
            int base = getSegmentNumber() * getMaxSegmentSize();
            for (int i=from; i<=to; i++) {
                retval.add(new Numeric<Double>(data[positions.get(i)-base]));
            }
        }
    }
}
