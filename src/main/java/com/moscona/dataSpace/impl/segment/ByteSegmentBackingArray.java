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

package com.moscona.dataSpace.impl.segment;

import com.moscona.dataSpace.IVectorSegmentBackingArray;

/**
 * Created: 12/14/10 5:36 PM
 * By: Arnon Moscona
 */
public class ByteSegmentBackingArray  implements IVectorSegmentBackingArray<Byte> {
    private static final long serialVersionUID = 3661725368073952924L;
    public byte[] data; // IMPORTANT: public so that its directly accessible to th rest of the implementation

    public ByteSegmentBackingArray(int segmentSize) {
        data = new byte[segmentSize];
    }

    /**
     * Really returns T, except that generics do not let you interrogate the generic template types because of erasure.
     *
     * @return
     */
    @Override
    public Class getBaseElementType() {
        return String.class;
    }

    @Override
    public long sizeInBytes() {
        return (long)data.length;
    }

    /**
     * Trims the backing array to size
     */
    @Override
    public void trim(int size) {
        byte[] oldData = data;
        data = new byte[size];
        System.arraycopy(oldData,0,data,0,size);
    }
}
