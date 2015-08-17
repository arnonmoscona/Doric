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

package com.moscona.dataSpace.impl.query.support;

import com.moscona.dataSpace.IBitMap;
import com.moscona.dataSpace.impl.segment.*;

/**
 * Created: Dec 9, 2010 9:35:05 AM
 * By: Arnon Moscona
 * Used by query term implementations that can work on an entire backing array
 */
public interface IArrayMatcher {
    public void match(DoubleSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(FloatVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(ByteVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(ShortVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(IntegerVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(LongVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(BooleanVectorSegmentBackingArray array, IBitMap progressiveResult);
//    public void match(StringVectorSegmentBackingArray array, IBitMap progressiveResult);

    public boolean canMatchRealNumbers();
    public boolean canMatchIntegralNumbers();
    public boolean canMatchStrings();
    public boolean canMatchBooleans();      
}
