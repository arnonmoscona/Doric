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

import java.io.Serializable;

/**
 * Created: 12/10/10 4:52 PM
 * By: Arnon Moscona
 * The standard set of type specific descriptive stats
 */
public interface IDescriptiveStats<T> extends Serializable {
    public T getMin();
    public T getMax();
    int getCount();

    /**
     * Other than min, max, and count - all the rest of the stats are only provided whwn hasMoments if true (ony numeric
     * vectors)
     * @return
     */
    public boolean hasMoments();
    public Double sum();
    public Double mean();
    public Double sumSquares();
    public Double variance();
    public Double stdev();

    /**
     * The on-line calculation of variance and stdev depends on the cumulative sum(squares), which can get large -
     * possibly getting to a range where the numbers lose precision. This flag will turn on when the sum of squares
     * crosses some threshold. Similarly a risk exists if the sum of squares is exceedingly low, but it is less
     * critical in most cases.
     * @return
     */
    public boolean secondMomentAccuracyFlag();
    //public Double skewness(); HOLD add support for skewness
    //public Double kurtosis(); HOLD add support for kurtosis
}
