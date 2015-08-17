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

/**
 * Created: 12/15/10 3:45 PM
 * By: Arnon Moscona
 * A simple been to represent a range of values.
 * Used mostly in query parameters, not intended for general use outside of it.
 * A null value on the left with a non-null on the right, open on the left and closed on the right is like a <= operator
 * (the left is taken as -infinity)
 * A similar pattern for the three other combinations, giving us <, <=, >, >=
 */
public class Range<T extends Comparable<T>> {
    private boolean isLeftInclusive=true;
    private boolean isRightInclusive=true;
    private T from;
    private T to;

    /**
     * The basic constructor. Usually you would use RangeFactory to make ranges. It's easier.
     * @param from
     * @param to
     * @param isLeftInclusive
     * @param isRightInclusive
     */
    public Range(T from, T to, boolean isLeftInclusive, boolean isRightInclusive) {
        this.from = from;
        this.to = to;
        this.isLeftInclusive = isLeftInclusive;
        this.isRightInclusive = isRightInclusive;
    }

    public T getFrom() {
        return from;
    }

    public void setFrom(T from) {
        this.from = from;
    }

    public boolean isLeftInclusive() {
        return isLeftInclusive;
    }

    public void setLeftInclusive(boolean leftInclusive) {
        isLeftInclusive = leftInclusive;
    }

    public boolean isRightInclusive() {
        return isRightInclusive;
    }

    public void setRightInclusive(boolean rightInclusive) {
        isRightInclusive = rightInclusive;
    }

    public T getTo() {
        return to;
    }

    public void setTo(T to) {
        this.to = to;
    }
}
