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

import com.moscona.dataSpace.persistence.PersistenceType;

/**
 * Created: 12/16/10 4:57 PM
 * By: Arnon Moscona
 */
public class DoubleSegmentStats extends AbstractSegmentStats<Double> {
    private static final long serialVersionUID = 2958448755617728377L;

    public DoubleSegmentStats(PersistenceType persistenceType) {
        super(persistenceType);
    }

    @Override
    protected Double maximum(Double max, Double max1) {
        return Math.max(max,max1);
    }

    @Override
    protected Double minimum(Double min, Double min1) {
        return Math.min(min,min1);
    }

    @Override
    public void add(Double element) {
        if (isFirstElement()) {
            min = max = element;
        }
        else {
            if (element<min)
                min = element;
            if (element>max)
                max = element;
        }
        accumulate(element);
    }

    /**
     * Other than min, max, and count - all the rest of the stats are only provided whwn hasMoments if true (ony numeric
     * vectors)
     *
     * @return
     */
    @Override
    public boolean hasMoments() {
        return true;
    }
}
