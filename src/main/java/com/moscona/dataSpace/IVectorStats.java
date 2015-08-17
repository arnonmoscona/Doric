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

/**
 * Created: 12/10/10 4:42 PM
 * By: Arnon Moscona
 * Overall vector statistics: descriptive and quantiles
 */
public interface IVectorStats<T extends IScalar> {
    void setQuantiles(IQuantiles quantiles);

    void setDescriptiveStats(IDescriptiveStats<T> stats);

    IDescriptiveStats<? extends Object> getDescriptiveStats();

    IQuantiles getQuantiles();
}
