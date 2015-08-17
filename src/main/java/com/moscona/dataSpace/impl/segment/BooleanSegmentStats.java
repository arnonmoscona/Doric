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
 * Created: 12/16/10 4:58 PM
 * By: Arnon Moscona
 */
public class BooleanSegmentStats extends AbstractSegmentStats<Boolean> {
    private static final long serialVersionUID = 537262971469229059L;

    public BooleanSegmentStats(PersistenceType persistenceType) {
        super(persistenceType);
    }

    @Override
    protected Boolean maximum(Boolean max, Boolean max1) {
        return !max && max1;
    }

    @Override
    protected Boolean minimum(Boolean min, Boolean min1) {
        return min && !min1;
    }

    @Override
    public void add(Boolean element) {
        if (isFirstElement()) {
            min = max = element;
        } else {
            if(min && !element)
                min= element;
            if(!max && element)
                max = element;
        }

    }


    @SuppressWarnings({"unchecked"})
    @Override
    public AbstractSegmentStats<Boolean> clone() throws CloneNotSupportedException {
        return (AbstractSegmentStats<Boolean>)super.clone();
    }
}
