/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.sparql.expr.aggregate;

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprEvalException;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.FunctionEnv;

/** Accumulator that passes down every value of an expression */
public abstract class AccumulatorExpr implements Accumulator {
    private long count = 0;
    private long errorCount = 0;
    private final Expr expr;

    protected AccumulatorExpr(Expr expr) {
        this.expr = expr;
    }

    @Override
    public void accumulate(Binding binding, FunctionEnv functionEnv) {
        try {
            NodeValue nv = expr.eval(binding, functionEnv);
            accumulate(nv, binding, functionEnv);
            incrementCount();
        } catch (ExprEvalException ex) {
            incrementErrorCount();
            accumulateError(binding, functionEnv);
        }
    }
    
    /**
     * Gets the expression
     * @return Expression
     */
    protected final Expr getExpr() {
        return expr;
    }

    /**
     * Increments the count of valid expression evaluations
     */
    protected final void incrementCount() {
        count++;
    }

    /**
     * Increments the count of error expression evaluations
     */
    protected final void incrementErrorCount() {
        errorCount++;
    }

    /**
     * Gets the value of the accumulator
     */
    @Override
    public NodeValue getValue() {
        if (errorCount == 0)
            return getAccValue();
        return getErrorValue();
    }

    /**
     * Gets the value that is returned in the event of any errors (defaults to
     * {@code null})
     * 
     * @return Error value
     */
    protected NodeValue getErrorValue() {
        return null;
    }

    /**
     * Gets the count of valid expression evaluations
     * 
     * @return Valid expression evaluation count
     */
    protected final long getCount() {
        return count;
    }

    /**
     * Gets the count of expression evaluation errors encountered
     * 
     * @return Expression evaluation error count
     */
    protected final long getErrorCount() {
        return errorCount;
    }

    /** Called if no errors to get the accumulated result */
    protected abstract NodeValue getAccValue();

    /**
     * Called when the expression being aggregated evaluates OK
     * <p>
     * Can throw {@link ExprEvalException} in which case the
     * {@link #accumulateError} method is called
     * </p>
     */
    protected abstract void accumulate(NodeValue nv, Binding binding, FunctionEnv functionEnv);

    /**
     * Called when an evaluation of the expression causes an error or when the
     * accumulation step throws {@link ExprEvalException}
     */
    protected abstract void accumulateError(Binding binding, FunctionEnv functionEnv);
}
