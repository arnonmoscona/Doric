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

package com.moscona.dataSpace.impl.query;

import com.moscona.dataSpace.IPositionIterator;
import com.moscona.dataSpace.exceptions.DataSpaceException;

/**
 * Created: 5/18/11 3:01 PM
 * By: Arnon Moscona
 * A class that helps bulk evaluations that are filtered using a selection.
 * Assumption: the bulk evaluation calls the isNextSelected() method on every segment element
 */
public class FilteredQueryHelper {
    int index;
    private int selectedIndex;
    private IPositionIterator positionIterator;
    private boolean done = false;

    public FilteredQueryHelper(int startIndex, int nextSelectedIndex, IPositionIterator positionIterator) {
        selectedIndex = nextSelectedIndex;
        this.positionIterator = positionIterator;
        index = startIndex;
    }

    public boolean isNextSelected() throws DataSpaceException {
        int testValue = index++;

        if (done) {
            return false;
        }

        boolean result = (testValue == selectedIndex);
        if (result) {
            // advance the next selected
            if (positionIterator.hasNext()) {
                selectedIndex = positionIterator.next();
            }
            else {
                done = true;
            }
        }
        return result;
    }

    public boolean isDone() {
        return done;
    }
}
