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
 * Created: 12/21/10 2:10 PM
 * By: Arnon Moscona
 * Support for numeric (float ,double) vector resolution. For real numbers equality in the vector is up to the vector's
 * resolution.
 *     a.equals(b) iff a-b <= resolution
 * This is used to generate unique values as well as in tests of equality in range, equality or
 * other comparison queries.
 * The resolution does not affect individual values, they maintain the original precision, only value comparisons
 * are affected.
 *     e.equals(b) =/=> a==b
 */
public interface INumericResolutionSupport {
    /**
     * Is vector resolution automatically determined (by default true)
     * @return true if the resolution was determined automatically
     */
    public boolean isAutoResolution();

    /**
     * Based on the resolution, is there more than one distinct value in the vector?
     * @return
     */
    public boolean hasMoreThanOneValue() throws DataSpaceException;

    /**
     * Gets the vector's resolution
     * @return
     */
    public double getResolution();

    /**
     * sets the vector's resolution. Can only be done before it is sealed
     * @param resolution
     */
    public void setResolution(double resolution) throws DataSpaceException;
}
