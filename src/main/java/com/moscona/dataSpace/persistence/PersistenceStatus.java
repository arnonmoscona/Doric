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

package com.moscona.dataSpace.persistence;

/**
 * Created: 12/9/10 3:36 PM
 * By: Arnon Moscona
 */
public enum PersistenceStatus {
    NOT_PERSISTED,  // does not have a disk copy
    SWAPPED_IN,     // has a disk copy and is present in memory
    SWAPPED_OUT     // has a disk copy but is not present in memory
}
