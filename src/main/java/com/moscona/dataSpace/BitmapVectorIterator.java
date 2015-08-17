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
 * Created: 12/17/10 4:03 PM
 * By: Arnon Moscona
 */
public class BitmapVectorIterator<T extends IScalar> implements IVectorIterator<T> {
    private IBitMap bitMap;
    private IVector<T> vector;
    private IPositionIterator position;
    private int maxPosition;
    private int lastReturnedPosition = -1;

    public BitmapVectorIterator(IVector<T> vector, IBitMap bitMap) {
        this.bitMap = bitMap;
        this.vector = vector;
        maxPosition = vector.size()-1;
        position = bitMap.getPositionIterator();
    }

    @Override
    public boolean hasNext() {
        return position.hasNext() && lastReturnedPosition<=maxPosition;
    }

    @Override
    public T next() throws DataSpaceException {
        if (lastReturnedPosition>=maxPosition) {
            return null;
        }
        return vector.get(position.next());
    }
}
