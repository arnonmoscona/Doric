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

package com.moscona.dataSpace.util;

import com.moscona.dataSpace.IBitMap;
import com.moscona.dataSpace.IPositionIterator;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import javaewah.EWAHCompressedBitmap;
import javaewah.IntIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * Created: Dec 6, 2010 12:02:37 PM
 * By: Arnon Moscona
 * An implementation of IBitMap that uses EWAHCompressedBitmap under the Apache 2.0 license for
 * the actual implementation.
 * Note that it is not generally useful in a mix with other implementations of IBitMap - it assumes that it always
 * interacts with bitmaps of the same class.
 */
public class CompressedBitMap implements IBitMap {
    private int lastPosition = -1;
    private EWAHCompressedBitmap bitmap;

    public CompressedBitMap() {
        lastPosition = -1;
        bitmap = new EWAHCompressedBitmap();    
    }

    private CompressedBitMap(EWAHCompressedBitmap actual) {
        lastPosition = actual.sizeInBits()-1;
        bitmap = actual;
    }

    @Override
    public IBitMap and(IBitMap other) {
        return new CompressedBitMap(bitmap.and(((CompressedBitMap)other).bitmap));
    }

    @Override
    public IBitMap or(IBitMap other) {
        return new CompressedBitMap(bitmap.or(((CompressedBitMap)other).bitmap));
    }

    @Override
    public IBitMap not() {
        try {
            EWAHCompressedBitmap clone = (EWAHCompressedBitmap)bitmap.clone();
            clone.not();
            return new CompressedBitMap(clone);
        }
        catch (CloneNotSupportedException e) {
            return null;
        }
    }

    @Override
    public IBitMap add(boolean value) {
        lastPosition++;
        if (value) {
            bitmap.set(lastPosition);
        }
        return this;
    }

    @Override
    public int size() {
        return Math.max(lastPosition, bitmap.sizeInBits());
    }

    @Override
    public IPositionIterator getPositionIterator() {
        return new Iterator();
    }

    /**
     * the total number of true values
     */
    @Override
    public int cardinality() {
        // may be more efficient to just count in the add() method, but as soon as you combine bitmaps - you lose this
        return bitmap.cardinality();
    }

//    public IntIterator debugEwahIterator() {
//        return bitmap.intIterator();
//    }
//
//    public java.util.Iterator<Integer> debugEwahIntegerIterator() {
//        return bitmap.iterator();
//    }
//
//    public boolean[] debugDump(int size) throws DataSpaceException {
//        boolean[] retval = new boolean[size];
//        IPositionIterator iterator = getPositionIterator();
//        while (iterator.hasNext()) {
//            int pos = iterator.next();
//            retval[pos] = true;
//        }
//        return retval;
//    }

    //------------------------------------------------------------------------------------------------------------------

    private class Iterator implements IPositionIterator {
        private IntIterator delegate;
        private int lastReturnedValue = Integer.MIN_VALUE;

        protected Iterator() {
            delegate = bitmap.intIterator();
        }

        @Override
        public int next() {
            lastReturnedValue = delegate.next();
            return lastReturnedValue;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public int fastForwardPast(int lastIndex, int resultIfNoMoreValues) {
            while (delegate.hasNext()) {
                int result = delegate.next();
                if (result>lastIndex) {
                    return result;
                }
            }
            return resultIfNoMoreValues;
        }

        @Override
        public int lastReturnedValue() {
            return lastReturnedValue;
        }
    }

    /**
     * A temporary replacement for Iterator. See #IT-511
     */
    @SuppressWarnings({"UseOfObsoleteCollectionType"})
    private class VectorBasedIterator implements IPositionIterator {
        private List<Integer> positionVector;
        private java.util.Iterator<Integer> positionIterator;
        private int lastReturnedValue = Integer.MIN_VALUE;

        protected VectorBasedIterator() {
            positionVector = bitmap.getPositions();
            positionIterator = positionVector.iterator();
        }

        @Override
        public int next() throws DataSpaceException {
            lastReturnedValue = positionIterator.next();
            return lastReturnedValue;
        }

        @Override
        public boolean hasNext() {
            return positionIterator.hasNext();
        }

        @Override
        public int fastForwardPast(int lastIndex, int resultIfNoMoreValues) {
            while (positionIterator.hasNext()) {
                int result = positionIterator.next();
                if (result > lastIndex) {
                    return result;
                }
            }
            return resultIfNoMoreValues;
        }

        @Override
        public int lastReturnedValue() {
            return lastReturnedValue;
        }
    }

    @Override
    public List<Integer> getPositions() {
        return new ArrayList<Integer>(bitmap.getPositions());
    }
}
