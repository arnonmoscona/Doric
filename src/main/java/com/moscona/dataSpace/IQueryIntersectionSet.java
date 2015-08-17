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
 * Created: 12/10/10 11:43 AM
 * By: Arnon Moscona
 * A set of tuples {query term, parameters, column} which get intersected with an AND. This is the typical OLAP query
 * and it can be optimized by arbitrary order by first evaluating the IO cost of each term on each vector without
 * actually performing IO (evaluation only on segment stats)
 */
public interface IQueryIntersectionSet {
    public IQueryIntersectionSet add(IQueryTerm term, IQueryParameterList parameters, String columnName);
    public int size();
    public IQueryTerm getTerm(int i);
    public IQueryParameterList getParameterList(int i);
    public String getColumnName(int i);

    IBitMap applyTerm(int i, IVector vector, IQueryState queryState, IBitMap cumulativeResult) throws DataSpaceException;
}
