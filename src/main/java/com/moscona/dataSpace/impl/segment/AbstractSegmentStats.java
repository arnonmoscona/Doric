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

import com.moscona.dataSpace.ISegmentStats;
import com.moscona.dataSpace.exceptions.DataSpaceException;
import com.moscona.dataSpace.persistence.PersistenceType;

/**
 * Created: 12/13/10 11:37 AM
 * By: Arnon Moscona
 */
public abstract class AbstractSegmentStats<T> implements ISegmentStats<T>, Cloneable {
    private static final long serialVersionUID = -7300322383878699910L;
    private PersistenceType persistenceType;
    private int count = 0;
    protected T min = null;
    protected T max = null;

    private double sum = 0.0; // for moment calculation only
    private double sumSquares = 0.0; // for moment calculation only

    public AbstractSegmentStats(PersistenceType persistenceType) {
        this.persistenceType = persistenceType;
    }

    protected abstract T maximum(T max, T max1);

    protected abstract T minimum(T min, T min1);

    @Override
    public PersistenceType getPersistenceType() {
        return persistenceType;
    }

    protected final boolean isFirstElement() {
        count++;
        return count==1;
    }

    @Override
    public int getCount() {
        return count;
    }

    /**
     * Changes the persistence type for the value.
     *
     * @param persistenceType
     */
    @Override
    public void setPersistenceType(PersistenceType persistenceType) {
        this.persistenceType = persistenceType;
    }

    @Override
    public long sizeInBytes() throws DataSpaceException {
        return 0;  // not memory managed
    }

    @Override
    public T getMax() {
        return max;
    }

    @Override
    public T getMin() {
        return min;
    }

    public void setMax(T max) {
        this.max = max;
    }

    public void setMin(T min) {
        this.min = min;
    }

    /**
     * Other than min, max, and count - all the rest of the stats are only provided whwn hasMoments if true (ony numeric
     * vectors)
     *
     * @return
     */
    @Override
    public boolean hasMoments() {
        return false;
    }

    @Override
    public Double sum() {
        return hasMoments() ? sum : null;
    }

    @Override
    public Double mean() {
        return hasMoments() ? sum/count : null;
    }

    @Override
    public Double sumSquares() {
        return hasMoments() ? sumSquares : null;
    }

    @Override
    public Double variance() {
        if (! hasMoments()) {
            return null;
        }
        double mean = mean();
        return (count==0)?0.0:((double)sumSquares - 2.0*mean*sum + count*mean*mean)/count;
    }

    @Override
    public Double stdev() {
        if (! hasMoments()) {
            return null;
        }
        return (count==0)?0.0:Math.sqrt(variance());
    }

    /**
     * The on-line calculation of variance and stdev depends on the cumulative sum(squares), which can get large -
     * possibly getting to a range where the numbers lose precision. This flag will turn on when the sum of squares
     * crosses some threshold. Similarly a risk exists if the sum of squares is exceedingly low, but it is less
     * critical in most cases.
     *
     * @return
     */
    @Override
    public boolean secondMomentAccuracyFlag() {
        return sumSquares > Integer.MAX_VALUE; // HOLD (fix before release)  this is an arbitrary selection at this point
    }

    /**
     * Used by numeric types to collect the on-line values used for moment calculations
     * @param sample
     */
    protected void accumulate(double sample) {
        sum += sample;
        sumSquares += sample*sample;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public AbstractSegmentStats<T> clone() throws CloneNotSupportedException {
        return (AbstractSegmentStats<T>)super.clone();
    }

    public void add(AbstractSegmentStats<T> stats) {
        if (count > 0) {
            if (stats.count>0) {
                min = minimum(min,stats.min);
                max = maximum(max, stats.max);
            }
        }
        else {
            min = stats.min;
            max = stats.max;
        }
        count += stats.count;
        if (hasMoments()) {
            sum += stats.sum;
            sumSquares += stats.sumSquares();
        }
    }
}
