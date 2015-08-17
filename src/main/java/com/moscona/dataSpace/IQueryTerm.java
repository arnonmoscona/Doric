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
 * Created: Dec 8, 2010 6:20:32 PM
 * By: Arnon Moscona
 */
public interface IQueryTerm<T extends IScalar> {
    /**
     * Creates an empty parameter list that is appropriate for this term.
     * @return
     */
    public IQueryParameterList createParameterList(IVector.BaseType baseType) throws DataSpaceException;

    /**
     * Applies the query using the parameters (note that in some queries parameters are supplied by the constructor and
     * there is no formal parameter list)
     * @param param
     * @param vector
     * @param queryState tracks query performance etc.
     * @return
     * @throws DataSpaceException
     */
    public IBitMap apply(IQueryParameterList param, IVector<T> vector, IQueryState queryState) throws DataSpaceException;

    /**
     * Same as apply(params,vector,queryState) but also provides a bitmap to intersect the result with. The
     * implementation may use the bitmap to limit which elements are actually evaluated and avoid the potential IO
     * associated with evaluating data that is eliminated by the intersection bitmap.
     * @param param
     * @param vector
     * @param queryState
     * @param intersectWith
     * @return
     * @throws DataSpaceException
     */
    public IBitMap apply(IQueryParameterList param, IVector<T> vector, IQueryState queryState, IBitMap intersectWith) throws DataSpaceException;

    String toString(IQueryParameterList params);
}
