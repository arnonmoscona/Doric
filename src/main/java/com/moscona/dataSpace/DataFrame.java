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
import com.moscona.dataSpace.impl.AbstractVector;
import com.moscona.dataSpace.impl.IntegerVector;
import com.moscona.dataSpace.util.CompressedBitMap;
import com.moscona.exceptions.InvalidArgumentException;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

/**
 * Created: 12/9/10 3:39 PM
 * By: Arnon Moscona
 * A collection of named vectors (names in the data frame may differ from names in the data space)
 */
public class DataFrame extends AbstractDataElement implements IDataElement {
    private static final long serialVersionUID = -1952904026775489258L; // incompatible change
    public static final String PRIMARY_SORT_COLUMN = "primarySortColumn";
    private HashMap<String, Entry> columns;
    private ArrayList<String> order;
    private String rowIdColumn = null;
    private int size = 0;
    private DataSpace dataSpace;
    private HashMap<String,Object> metaData;

    public DataFrame(DataSpace dataSpace) {
        this.dataSpace = dataSpace;
        columns = new HashMap<String,Entry>();
        order = new ArrayList<String>();
        metaData = new HashMap<String,Object>();
    }

    /**
     * Adds a column to the data frame
     * @param name
     * @param column
     * @param isLabel true if this column is considered the a label (in the analytical sense - a label is an outcome,
     * whereas non-label columns are generally features)
     * @return this (for concatenation of cbind() operations)
     * @throws DataSpaceException
     */
    public DataFrame cbind(String name, IVector column, boolean isLabel) throws DataSpaceException {
        if (getNameSpace() != null) {
            ((DataBundle)getNameSpace()).enforceVectorMembershipRules(column);
        }
        IVector vector = column;
        if (vector.getDataSpace() != dataSpace) { // we insist on reference equality, not just equivalence
            vector = column.copyTo(dataSpace);
        }

        if (columns.containsKey(name)) {
            throw new DataSpaceException("A column named\""+name+"\" already exists in this data frame");
        }
        if (columns.size()>0 && vector.size() != size) {
            throw new DataSpaceException("The column size is different from the data frame size. Expected "+size+" but got "+vector.size()+" new column: "+name+" (existing columns : "+StringUtils.join(order,", ")+")");
        }

        columns.put(name, new Entry(vector, isLabel));
        order.add(name);
        size = vector.size();

        return this;
    }

    /**
     * Adds a non-label column to the data frame
     * @param name
     * @param column
     * @return
     * @throws DataSpaceException
     */
    public DataFrame cbind(String name, IVector column) throws DataSpaceException {
        return cbind(name, column, false);
    }

    public int size() {
        return size;
    }

    /**
     *
     * @param name
     * @return
     * @throws DataSpaceException
     */
    public DataFrame setId(String name) throws DataSpaceException {
        validateColumnExists(name);
        rowIdColumn = name;
        return this;
    }

    private void validateColumnExists(String name) throws DataSpaceException {
        validateNotEmpty();
        if (! columns.containsKey(name)) {
            throw new DataSpaceException("There is no column named \""+name+"\" in this data frame");
        }
    }

    private void validateColumnIndex(int i) throws DataSpaceException {
        validateNotEmpty();
        if (i<0 || i>=columns.size()) {
            throw new DataSpaceException("The column index "+i+ " does not exist in this data frame");
        }
    }

    private void validateRowIndex(int i) throws DataSpaceException {
        validateNotEmpty();
        if (i<0 || i>=getRowCount()) {
            throw new DataSpaceException("The row index "+i+ " does not exist in this data frame. Row count = "+getRowCount()+" row requested = "+i);
        }
    }

    private void validateNotEmpty() throws DataSpaceException {
        if (columns.size()==0) {
            throw new DataSpaceException("No columns in this data frame");
        }
    }

    public IVector get(String name) throws DataSpaceException {
        validateColumnExists(name);
        return columns.get(name).vector;
    }

    public IVector get(int column) throws DataSpaceException {
        validateColumnIndex(column);
        return get(order.get(column));
    }

    public int getRowCount() throws DataSpaceException {
        validateNotEmpty();
        return size;
    }

    public IScalar get(int row, String column) throws DataSpaceException {
        validateColumnExists(column);
        validateRowIndex(row);
        return get(column).get(row);
    }

    public IScalar get(int row, int column) throws DataSpaceException {
        validateColumnIndex(column);
        return get(row, order.get(column));
    }

    public Map<String, IScalar> getRow(int row, Collection<String> requestedColumns) throws DataSpaceException {
        validateRowIndex(row);
        HashMap<String,IScalar> retval = new HashMap<String, IScalar>();
        Collection<String> list = (requestedColumns==null ? order : requestedColumns);
        for (String column: list) {
            retval.put(column, columns.get(column).vector.get(row));
        }
        return retval;
    }

    public Map<String, IScalar> getRow(int row) throws DataSpaceException {
        return getRow(row, null);
    }


    public IVectorIterator<Map<String, IScalar>> iterator() throws DataSpaceException {
        validateAllVectorsAreSealed();
        return new RowIterator(this);
    }

    public IVectorIterator<Map<String, IScalar>> iterator(Collection<String> columns) throws DataSpaceException, InvalidArgumentException {
        validateAllVectorsAreSealed();
        return new RowIterator(this, columns);
    }

    private synchronized void validateAllVectorsAreSealed() throws DataSpaceException {
        for (String entry: columns.keySet()) {
            AbstractVector vector = (AbstractVector) (columns.get(entry)).vector;
            if (! vector.isSealed()) {
                throw new DataSpaceException("Error: attempt to iterate while at least one vector is not sealed: "+entry);
            }
        }
    }

    public IVectorIterator<Map<String, IScalar>> iterator(IBitMap result) throws DataSpaceException {
        if (result==null) {
            return iterator();
        }
        return new BitMapRowIterator(this, result);
    }

    public IVectorIterator<Map<String, IScalar>> iterator(IBitMap result, Collection<String> columns) throws DataSpaceException, InvalidArgumentException {
        if (result==null) {
            return iterator(columns);
        }
        return new BitMapRowIterator(this, result, columns);
    }

    public IVectorIterator<Map<String, IScalar>> iterator(IBitMap result, String... columns) throws DataSpaceException, InvalidArgumentException {
        if (columns == null || columns.length==0) {
            return iterator(result);
        }
        else {
            ArrayList<String> list = new ArrayList<String>(columns.length);
            Collections.addAll(list, columns);
            return iterator(result, list);
        }
    }

    public boolean isLabel(String column) throws DataSpaceException {
        validateColumnExists(column);
        return columns.get(column).isLabel;
    }

    /**
     * Identifies the column that is set as the row ID (if exists)
     * @return the name of the column
     */
    public String getRowId() {
        return rowIdColumn;
    }

    /**
     * Returns the row ID value for a specific row
     * @param row the row number
     * @return
     * @throws DataSpaceException
     */
    public IScalar getRowId(int row) throws DataSpaceException {
        validateRowIndex(row);
        if (rowIdColumn==null) {
            return null;
        }
        return columns.get(rowIdColumn).vector.get(row);
    }

    /**
     * Given a bit map of the same length as the data frame, produces a new data frame matching only the true entries in the bit map
     * @param bitmap
     * @return
     */
    public DataFrame subset(IBitMap bitmap) throws DataSpaceException {
        DataFrame retval = new DataFrame(dataSpace);

        for (String col: order) {
            Entry entry = columns.get(col);
            IVector subset = entry.vector.subset(bitmap);
            retval.cbind(col, subset, entry.isLabel);
        }

        retval.rowIdColumn = rowIdColumn;
        return retval;
    }



    /**
     * Converts the selected column into a factor and replaces the original column with the appropriate FactorValueVector
     * @param columnName
     */
    public void factor(String columnName) throws DataSpaceException {
        IVector column = get(columnName);
        column.factor(columnName+" factor");
        // replaceEntry(columnName, newColumn);  //IMPORTANT - decided to get rid of IFactorValueRecord and simply mark vectors as factors for now
    }

    private void replaceEntry(String columnName, IVector column) {
        Entry entry = columns.get(columnName);
        entry.vector = column;
    }

    @Override
    public long sizeInBytes() {
        return 0;  // do nothing. The data frame is not memory managed
    }

    public IBitMap select(IQueryIntersectionSet query) throws DataSpaceException {
        // concurrency HOLD (fix before release) #IT-477 need to obtain a read lock here - maybe just create a synchronized block around getting a copy of the column name->vector mapping prior to query execution. This way we have a brief lock that would get the "image" of the data frame that would be stable for the duration of the query and would not require holding any long lived lock.
        // validate columns
        for (int i=0; i<query.size(); i++) {
            String column = query.getColumnName(i);
            if (! columns.containsKey(column)) {
                throw new DataSpaceException("Query references non-existing column \""+column+"\" query: \""+query+"\", data frame columns: "+columnsAsString());
            }
        }

        // decide on resolution order
        ArrayList<Integer> executionOrder = resolveExecutionOrder(query);

        // execute
        IBitMap finalResult = null;
        for (int i: executionOrder) {
            String column = query.getColumnName(i);
            IQueryTerm term = query.getTerm(i);
            QueryState queryState = new QueryState();
            IBitMap termResult = query.applyTerm(i, columns.get(column).vector, queryState, finalResult);
            if (finalResult == null) {
                finalResult = termResult; // first term to be executed
            }
            else {
                finalResult = finalResult.and(termResult);
                // HOLD (fix before release)  need to go into javaEWAH and create a method that checks whether the bit map is all true or all false efficiently (faster than cardinality()) - contribute back to Daniel Lemire)
                if (finalResult.cardinality() == 0) {
                    break; // we narrowed down the result to nothing - no need to evaluate any further
                }
            }
        }

        return finalResult;
    }

    private ArrayList<Integer> resolveExecutionOrder(IQueryIntersectionSet query) {
        ArrayList<Integer> retval = new ArrayList<Integer>();
        for (int i=0; i<query.size(); i++) {
            // HOLD (fix before release)  query order optimization by selectivity estimate (using column stats and rough estimate of selectivity by each term) and grouping same columns
            retval.add(i);
        }
        return retval;
    }

    private String columnsAsString() {
        return StringUtils.join(order, ", ");
    }

    public Set<String> getColumnNames() {
        return columns.keySet();
    }

    /**
     * Advisory metadata (unverified) telling the user that the data has a primary sort on the specified column.
     * This is a contract between the creator and the user.
     * @param column
     * @throws DataSpaceException if the column does not exist
     */
    public void setSortColumn(String column) throws DataSpaceException {
        validateColumnExists(column);
        metaData.put(PRIMARY_SORT_COLUMN, column);
    }


    public String getSortColumn() {
        Object col = metaData.get(PRIMARY_SORT_COLUMN);
        return col==null? null : col.toString();
    }

    /**
     * Advisory metadata (unverified) telling the user that the data has a primary sort column.
     * This is a contract between the creator and the user.
     * @return
     */
    public boolean isSorted() {
        String sortColumn = getPrimarySortColumn();
        return sortColumn != null;
    }

    /**
     * Advisory metadata (unverified) telling the user that the data has a primary sort on the specified column.
     * This is a contract between the creator and the user.
     * @return the name of the sort column or null (indicating no sort)
     */
    private String getPrimarySortColumn() {
        return (String) metaData.get(PRIMARY_SORT_COLUMN);
    }

    /**
     * Create an integer vector populated with a the row numbers. Sets the column as the ID column
     * @param columnName
     * @throws DataSpaceException if there are no vectors in the data frame, or if there is already a column of the specified name
     */
    public IntegerVector createIdColumn(String columnName) throws DataSpaceException {
        if (columns.size()==0) {
            throw new DataSpaceException("There are no column in this data frame. Cannot tell what length the ID column should be");
        }
        if (order.contains(columnName)) {
            throw new DataSpaceException("This data frame already contains a column named \""+columnName+"\"");
        }

        // create the column
        IntegerVector id = new IntegerVector(dataSpace);
        id.setPersistenceType(getPersistenceType());

        id.setDescription("ID column (row number)");
        for (int i=0; i<size(); i++) {
            id.append(i);
        }
        id.seal();

        // figure out what name space it should be in
        INameSpace nameSpace = getNameSpace();
        if (nameSpace!=null) {
            nameSpace.add(id);
        }

        //add the column and mark it as the ID
        cbind(columnName, id);
        setId(columnName);

        return id;
    }

    @Override
    public DataFrame setDescription(String description) {
        super.setDescription(description);
        return this;
    }

    public double estimateMemoryLoad() throws DataSpaceException {
        double totalBytesForFirstSegments = 0.0;
        for (String column: columns.keySet()) {
            totalBytesForFirstSegments += columns.get(column).vector.getFirsSegmentSizeInBytes();
        }
        return totalBytesForFirstSegments / dataSpace.getMemoryManager().getMaxSize();
    }

    // ----------------------------------------------------------------------------------------------------------------

    private class Entry implements Serializable {
        private static final long serialVersionUID = -4284435669643449962L;
        public IVector vector;
        public boolean isLabel;

        private Entry(IVector vector, boolean label) {
            isLabel = label;
            this.vector = vector;
        }
    }
}
