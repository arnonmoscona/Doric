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
 * Created: Dec 9, 2010 9:31:34 AM
 * By: Arnon Moscona
 * Used to match a single value from a vector. The lowest level of a query term implementation.
 */
public interface IValueMatcher {
    public boolean matches(byte value);
    public boolean matches(short value);
    public boolean matches(int value);
    public boolean matches(long value);
    public boolean matches(float value);
    public boolean matches(double value);
    public boolean matches(String value);
    public boolean matches(boolean value);
    
    public boolean canMatch(byte value);
    public boolean canMatch(short value);
    public boolean canMatch(int value);
    public boolean canMatch(long value);
    public boolean canMatch(float value);
    public boolean canMatch(double value);
    public boolean canMatch(String value);
    public boolean canMatch(boolean value);
}
