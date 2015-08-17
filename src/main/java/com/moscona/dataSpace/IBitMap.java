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

import java.util.List;

/**
 * Created: Dec 6, 2010 11:43:07 AM
 * By: Arnon Moscona
 * A facade interface for bitmap implementations
 */
public interface IBitMap {
    public IBitMap and(IBitMap other);
    public IBitMap or(IBitMap other);
    public IBitMap not();
    public IBitMap add(boolean value);
    // IMPORTANT: not providing public boolean get(int index) - use iterator instead
    public int size();

    /**
     * Gets an iterator that iterates over true positions
     * @return
     */
    public IPositionIterator getPositionIterator();

    /**
     * the total number of true values
     */
    public int cardinality();

    public List<Integer> getPositions();
}
