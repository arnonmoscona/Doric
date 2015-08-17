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

import java.util.List;

/**
 * Created: 1/6/11 9:47 AM
 * By: Arnon Moscona
 * An interface for classes that build vectors
 */
public interface IVectorBuilder<T extends IScalar> {
    /**
     * Appends a single element to the vector
     * @param element
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException if the vector is sealed
     */
    public IVector<T> append(T element) throws DataSpaceException;

    /**
     * Appends all the elements of the other vector to the end of this vector.
     * Note that this is more efficient than append(IVector<T> vector) or append(List<T> vector) as implementations can
     * (and do) use direct access to the backing array.
     * @param vector
     * @return
     * @throws com.moscona.dataSpace.exceptions.DataSpaceException if the vector is sealed
     */
    public IVector<T> append(IVector<T> vector) throws DataSpaceException;

    public IVector<T> append(T[] vector) throws DataSpaceException;
    public IVector<T> append(List<T> vector) throws DataSpaceException;

    /**
     * Closes the vector and makes it immutable
     */
    public IVector<T> seal() throws DataSpaceException;

    public IDataElement setDescription(String description);

    public IVector.BaseType getBaseType();

    public IVector getVector() throws DataSpaceException;
}
