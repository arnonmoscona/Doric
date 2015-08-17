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

import com.moscona.dataSpace.persistence.IStorable;

/**
 * Created: 12/10/10 1:14 PM
 * By: Arnon Moscona
 * An object that holds the statistics of a vector segment and can be used to
 * -- potentially resolve a query on an entire segment without requiring its backing array (key performance optimization)
 * -- estimate the rough likely query outcome without requiring its backing array (not sure this would be needed)
 */
public interface ISegmentStats<T> extends IStorable, IDescriptiveStats<T> {
    public void add(T element);
    // HOLD compute segment quantiles (on seal) #IT-464 #IT-468
}
