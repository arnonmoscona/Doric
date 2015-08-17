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

import com.moscona.dataSpace.persistence.IMemoryManaged;

import java.io.Serializable;

/**
 * Created: 12/9/10 11:10 AM
 * By: Arnon Moscona
 * A marker interface for backing arrays. As the useful stuff is in a primitive array, there is no functionality
 * exposed. You must (unsafely) cast to a known implementation.
 * Using these encapsulations of primitive arrays roughly doubles access time compared to direct access to a primitive
 * array - presumably because of it being {object member access + array index access} - but it allows us to ship
 * around references to an array while avoiding array copies (can pass as argument to methods etc) - basically an array
 * pointer.
 * Vector segment backing arrays should have a long-term stable interface as they are the core of vector persistence.
 */
public interface IVectorSegmentBackingArray<T> extends Serializable {
    /**
     * Really returns T, except that generics do not let you interrogate the generic template types because of erasure.
     * @return
     */
    public Class getBaseElementType();

    public long sizeInBytes();

    /**
     * Trims the backing array to size
     */
    public void trim(int size);
}
