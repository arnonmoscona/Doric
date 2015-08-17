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

import java.util.Iterator;

/**
 * Created: Dec 6, 2010 11:56:22 AM
 * By: Arnon Moscona
 * Given a vector or data frame with a list of positions satisfying some criterion, this allows us to iterate over the
 * positions in the vector that contain values that satisfy the criterion. Most commonly this is used to iterate over
 * the true position of a boolean vector.
 * It is intentionally not Iterator<Integer> to support maximally efficient, immutable implementations that do not
 * support removal.
 */
public interface IPositionIterator {
    public int next() throws DataSpaceException;

    /**
     * Next true position
     * @return
     */
	public boolean hasNext();

    public int fastForwardPast(int lastIndex, int resultIfNoMoreValues);

    /**
     * The last value returned by next(). Undefined if iteration has not started yet
     * @return
     */
    int lastReturnedValue();
}
