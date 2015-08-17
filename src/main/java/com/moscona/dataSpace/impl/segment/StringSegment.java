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
 * Created: 12/13/10 11:05 AM
 * By: Arnon Moscona
 */
public class StringSegment extends AbstractVectorSegment<Text,String> {
    private static final long serialVersionUID = -2027859605435087893L;
    private long bytePerPointer;

    public StringSegment(DataSpace dataSpace, PersistenceType persistenceType, int maxSegmentSize) {
        super(dataSpace, persistenceType, maxSegmentSize, new StringSegmentStats(persistenceType));
        bytePerPointer = 5; // as opposed to new UndocumentedJava().bytesPerPointer() - concluded by experimentation on a 64bit JVM
    }

    StringSegmentBackingArray myBackingArray() {
        return (StringSegmentBackingArray)getBackingArray();
    }

    public void append(String value) throws DataSpaceException {
        // IMPORTANT - we are still mutable here, and so the memory manager is not involved with this segment yet
        myBackingArray().data[size()] = getDataSpace().getCode(value);
        incSize(1);
        ((StringSegmentStats) stats).add(value);
    }

    /**
     * Used by segment queries that are not deeply integrated with the native segment class. Iteration increases
     * query time for range queries (over direct access to the backing array) by an estimated multiplier of 4-5
     *
     * @return
     */
    @Override
    public ISegmentIterator<Text> iterator() {
        return new BackingArrayIterator();
    }

    @Override
    public long sizeInBytes() {
        return bytePerPointer*size(); // ignore the size of the individual strings, only count object pointers
    }

    @Override
    public ISegmentStats calculateStats() {
        //HOLD (before release) implement StringSegment.calculateStats (finalize stats, calculate quantiles, histogram etc) - note that range (min/max) is already calculated in the add method... These would become more important when we fix #IT-468 and #IT-464 (a better vector quantile estimation)
        return stats;
    }

    @Override
    protected void trimBackingArray() {
        myBackingArray().trim(size());
    }

    @Override
    protected IVectorSegmentBackingArray<String> createBackingArray() {
        return new StringSegmentBackingArray(getMaxSegmentSize());
    }

    @Override
    public Set<Text> getUniqueValues() {
        HashSet<Text> retval = new HashSet<Text>();
        for (int i=0; i<size(); i++) {
            Text value = new Text(getDataSpace().decodeToString(myBackingArray().data[i]));
            retval.add(value);
        }
        return retval;
    }

    @Override
    public double[] copyAsDoubles() {
        return null; // undefined operation for this type
    }

    @Override
    public void estimateQuantilesOnRestOfSegments(Quantiles quantiles) {
        // not applicable
    }

    public class BackingArrayIterator implements ISegmentIterator<Text> {
        private int currentIndex = -1;

        @Override
        public boolean hasNext() {
            return currentIndex < size()-1;
        }

        @Override
        public Text next() {
            if (! hasNext()) {
                return null;
            }

            currentIndex++;
            return new Text(getDataSpace().decodeToString(myBackingArray().data[currentIndex]));
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
    public void appendValues(ArrayList<Text> retval, List<Integer> positions, Integer from, Integer to) {
        int[] data = ((StringSegmentBackingArray) getBackingArray()).data;
        if (positions==null) {
            for (int datum: data) {
                retval.add(new Text(getDataSpace().decodeToString(datum)));
            }
        }
        else {
            int base = getSegmentNumber() * getMaxSegmentSize();
            for (int i=from; i<=to; i++) {
                retval.add(new Text(getDataSpace().decodeToString(data[positions.get(i)-base])));
            }
        }
    }
}
