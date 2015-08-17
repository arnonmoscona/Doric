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
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

/**
 * Created: 12/20/10 10:33 AM
 * By: Arnon Moscona
 */
public class QueryIntersectionSet implements IQueryIntersectionSet {
    private DataFrame dataFrame;
    private ArrayList<Term> terms;
    private IQueryState queryState;

    public QueryIntersectionSet(DataFrame dataFrame) {
        this.dataFrame = dataFrame;
        terms = new ArrayList<Term>();
    }

    @Override
    public IQueryIntersectionSet add(IQueryTerm term, IQueryParameterList parameters, String columnName) {
        terms.add(new Term(term,parameters,columnName));
        return this;
    }

    @Override
    public int size() {
        return terms.size();
    }

    @Override
    public IQueryTerm getTerm(int i) {
        return terms.get(i).getTerm();
    }

    @Override
    public IQueryParameterList getParameterList(int i) {
        return terms.get(i).getParams();
    }

    @Override
    public String getColumnName(int i) {
        return terms.get(i).getColumnName();
    }

    @Override
    @SuppressWarnings({"unchecked"})  // the call to apply() is unchecked as you don't immediately know the types and the validation should make sure that all components of the query match the associated columns
    public IBitMap applyTerm(int i, IVector vector, IQueryState queryState, IBitMap cumulativeResult) throws DataSpaceException {
        // optimization hints for the term
        queryState.setCumulativeResult(cumulativeResult);
        queryState.setContextIsIntersectionGroup(true);

        IBitMap result = terms.get(i).getTerm().apply(terms.get(i).getParams(), vector, queryState);
        terms.get(i).setLastResult(result);
        this.queryState = queryState;
        return result;
    }

    public IQueryState getQueryState() {
        return queryState;
    }

    public void setQueryState(IQueryState queryState) {
        this.queryState = queryState;
    }

    @Override
    public String toString() {
        ArrayList<String> parts = new ArrayList<String>();
        for (Term term: terms) {
            parts.add(term.getColumnName() +" "+ term.getTerm().toString(term.getParams()));
        }
        return StringUtils.join(parts," AND ");
    }

    public class Term {
        private IQueryTerm term;
        private IQueryParameterList params;
        private String columnName;
        private IBitMap lastResult = null;

        protected Term(IQueryTerm term, IQueryParameterList params, String columnName) {
            this.term = term;
            this.params = params;
            this.columnName = columnName;
            queryState = new QueryState();
        }

        public String getColumnName() {
            return columnName;
        }

        public IQueryParameterList getParams() {
            return params;
        }

        public IQueryTerm getTerm() {
            return term;
        }

        public IQueryState getQueryState() {
            return queryState;
        }

        public IBitMap getLastResult() {
            return lastResult;
        }

        public void setLastResult(IBitMap lastResult) {
            this.lastResult = lastResult;
        }
    }
}
