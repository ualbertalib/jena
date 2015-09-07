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

/**
 * Accumulator that passes down the value of an expression but that allows for
 * short circuiting.
 * <p>
 * Short circuiting handles the case where an accumulator can determine the
 * final value ahead of seeing all the solutions. In this case it can indicate
 * as such by calling the {@link #shortCircuit(NodeValue)} method after which
 * point the evaluation of the expression is skipped for further solutions
 * </p>
 * 
 */
public abstract class AccumulatorShortCircuitExpr extends AccumulatorExpr {
    private boolean shortCircuited = false;
    private NodeValue shortCircuitValue;

    protected AccumulatorShortCircuitExpr(Expr expr) {
        super(expr);
    }

    @Override
    public void accumulate(Binding binding, FunctionEnv functionEnv) {
        // No need to do any further work if already short circuited
        if (shortCircuited)
            return;

        try {
            NodeValue nv = this.getExpr().eval(binding, functionEnv);
            accumulate(nv, binding, functionEnv);
            this.incrementCount();
        } catch (ExprEvalException ex) {
            this.incrementErrorCount();
            accumulateError(binding, functionEnv);
        }
    }

    // Count(?v) is different
    @Override
    public NodeValue getValue() {
        if (shortCircuited)
            return shortCircuitValue;
        if (this.getErrorCount() == 0)
            return getAccValue();
        return getErrorValue();
    }

    /**
     * Called when the derived class has been able to determine the final
     * aggregate value without needing to process further values. Once set
     * further expression evaluation is skipped and the final value will be the
     * value given here
     * 
     * @param value
     *            Final aggregate value
     */
    protected final void shortCircuit(NodeValue value) {
        this.shortCircuited = true;
        this.shortCircuitValue = value;
    }
}
