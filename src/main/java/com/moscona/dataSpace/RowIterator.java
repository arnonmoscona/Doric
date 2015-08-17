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
import com.moscona.exceptions.InvalidArgumentException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created: 12/20/10 4:41 PM
 * By: Arnon Moscona
 */
public class RowIterator implements IVectorIterator<Map<String, IScalar>> {
    private DataFrame dataFrame;
    private int nextRow;
    private HashSet<String> columns = null;

    RowIterator(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
        nextRow = 0;
    }

    RowIterator(DataFrame dataFrame, Collection<String> columns) throws InvalidArgumentException {
        this(dataFrame);
        this.columns = new HashSet<String>(columns);
        Set<String> allColumns = dataFrame.getColumnNames();
        for (String c: columns) {
            if (!allColumns.contains(c)) {
                throw new InvalidArgumentException("One of the requested columns \""+c+"\" is not in this data frame");
            }
        }
    }

    @Override
    public boolean hasNext() {
        return dataFrame.size()>nextRow;
    }

    @Override
    public Map<String, IScalar> next() throws DataSpaceException {
        if (columns==null) {
            return dataFrame.getRow(nextRow++);
        } else {
            return dataFrame.getRow(nextRow++, columns);
        }
    }
}
