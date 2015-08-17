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

import java.util.*;

/**
 * Created: 12/15/10 4:06 PM
 * By: Arnon Moscona
 * A convenience class to make various ranges
 */
public class RangeFactory {
    /**
     * Creates a closed range (inclusive on both ends)
     * @param from
     * @param to
     * @return
     */
    public Range<Byte> createRange(byte from, byte to) {
        return new Range<Byte>(from,to,true,true);
    }

    /**
     * Creates a closed range (inclusive on both ends)
     * @param from
     * @param to
     * @return
     */
    public Range<Short> createRange(short from, short to) {
        return new Range<Short>(from,to,true,true);
    }

    /**
     * Creates a closed range (inclusive on both ends)
     * @param from
     * @param to
     * @return
     */
    public Range<Integer> createRange(int from, int to) {
        return new Range<Integer>(from,to,true,true);
    }

    /**
     * Creates a closed range (inclusive on both ends)
     * @param from
     * @param to
     * @return
     */
    public Range<Long> createRange(long from, long to) {
        return new Range<Long>(from,to,true,true);
    }

    /**
     * Creates a right-open range (inclusive on left, exclusive on right)
     * @param from
     * @param to
     * @return
     */
    public Range<Float> createRange(float from, float to) {
        return new Range<Float>(from,to,true,false);
    }

    /**
     * Creates a right-open range (inclusive on left, exclusive on right)
     * @param from
     * @param to
     * @return
     */
    public Range<Double> createRange(double from, double to) {
        return new Range<Double>(from,to,true,false);
    }

    /**
     * Creates a list of right-open ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Float>> createRangeList(float[] boundaries, boolean isOpen) throws DataSpaceException {
        Float[] b2 = new Float[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }

    /**
     * Creates a list of right-open ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Double>> createRangeList(double[] boundaries, boolean isOpen) throws DataSpaceException {
        Double[] b2 = new Double[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }  

    /**
     * Creates a list of closed ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Long>> createRangeList(long[] boundaries, boolean isOpen) throws DataSpaceException {
        Long[] b2 = new Long[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }

    /**
     * Creates a list of closed ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Integer>> createRangeList(int[] boundaries, boolean isOpen) throws DataSpaceException {
        Integer[] b2 = new Integer[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }

    /**
     * Creates a list of closed ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Short>> createRangeList(short[] boundaries, boolean isOpen) throws DataSpaceException {
        Short[] b2 = new Short[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }

    /**
     * Creates a list of closed ranges ranging from -infinity to +infinity with the listed boundaries
     * @param boundaries
     * @param isOpen - if true will also include the ranges outside the boundaries
     * @return
     */
    public List<Range<Byte>> createRangeList(byte[] boundaries, boolean isOpen) throws DataSpaceException {
        Byte[] b2 = new Byte[boundaries.length];
        for (int i=0;i<boundaries.length;i++){b2[i] = boundaries[i];}
        return createRangeListInternal(b2, isOpen, false);
    }

    /**
     *
     * @param boundaries the list of boundaries from min to max must be monotonously increasing
     * @param isOpen if true will include the ranges outside the boundaries
     * @param integralStyle if true will create closed ranges. false will lead to right-open ranges
     * @param <T>
     * @return
     * @throws DataSpaceException
     */
    private <T extends Comparable<T>> List<Range<T>> createRangeListInternal(
            T[] boundaries, 
            boolean isOpen, 
            boolean integralStyle) throws DataSpaceException {
        if (boundaries.length==0) {
            throw new DataSpaceException("the boundary list has no items...");
        }

        List<Range<T>> list = new ArrayList<Range<T>>();
        Range<T> lastRange = null;

        T right = boundaries[0];
        if (isOpen) {
            list.add(new Range<T>(null, right, false, false));  // the range before the first
        }

        for (int i=1; i<boundaries.length; i++) {
            T left = right;
            right = boundaries[i];
            if (left.compareTo(right) <= 0) {
                throw new DataSpaceException("The boundary list is out of order "+left+" >= "+right);
            }
            lastRange = new Range<T>(left, right, true, integralStyle);
            list.add(lastRange);
        }

        if (isOpen) {
            list.add(new Range<T>(right, null, !integralStyle, false)); // the range after the last
        } else {
            // make the last interval closed on the right
            lastRange.setRightInclusive(true);
        }

        return list;
    }
    
}
