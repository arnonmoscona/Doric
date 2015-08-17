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

package com.moscona.dataSpace.impl;

import com.moscona.dataSpace.IScalar;
import com.moscona.dataSpace.IVector;
import com.moscona.dataSpace.IVectorIterator;
import com.moscona.dataSpace.exceptions.DataSpaceException;

/**
 * Created: 12/17/10 3:56 PM
 * By: Arnon Moscona
 */
public class SimpleVectorIterator<T extends IScalar> implements IVectorIterator<T> {
    private IVector<T> vector;
    private int lastIndex;
    private int size;

    public SimpleVectorIterator(IVector<T> vector) {
        this.vector = vector;
        lastIndex = -1;
        size = vector.size(); // can cache this because the vector is immutable
    }

    @Override
    public boolean hasNext() {
        return (lastIndex+1 < size);
    }

    @Override
    public T next() throws DataSpaceException {
        lastIndex++;
        return vector.get(lastIndex);
    }
}
