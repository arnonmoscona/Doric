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
public class StringSegmentStats extends AbstractSegmentStats<String> {
    private static final long serialVersionUID = -5792619265685403271L;

    public StringSegmentStats(PersistenceType persistenceType) {
        super(persistenceType);
    }

    @Override
    protected String maximum(String max, String max1) {
        return max.compareTo(max1)>0 ? max : max1;
    }

    @Override
    protected String minimum(String min, String min1) {
        return min.compareTo(min1) < 0 ? min :min1;
    }

    @Override
    public void add(String element) {
        if (isFirstElement()) {
            min = max = element;
        }
        else {
            if (element.compareTo(min) < 0)
                min = element;
            if (element.compareTo(max) > 0)
                max = element;
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public AbstractSegmentStats<String> clone() throws CloneNotSupportedException {
        return (AbstractSegmentStats<String>)super.clone();
    }
}
