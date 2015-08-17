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

import java.io.Serializable;

/**
 * Created: 12/14/10 12:58 PM
 * By: Arnon Moscona
 */
public class VectorStats<T extends IScalar> implements IVectorStats<T>, Serializable {
    private static final long serialVersionUID = -3469486487577496386L;
    private IQuantiles quantiles = null;
    private IDescriptiveStats<T> descriptiveStats = null;

    @Override
    public IDescriptiveStats<T> getDescriptiveStats() {
        return descriptiveStats;
    }

    @Override
    public void setDescriptiveStats(IDescriptiveStats<T> descriptiveStats) {
        this.descriptiveStats = descriptiveStats;
    }

    @Override
    public IQuantiles getQuantiles() {
        return quantiles;
    }

    @Override
    public void setQuantiles(IQuantiles quantiles) {
        this.quantiles = quantiles;
    }
}
