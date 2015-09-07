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
package org.apache.jena.sparql.function.library.leviathan.aggregates;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;
import org.apache.jena.sparql.expr.aggregate.AccumulatorShortCircuitExpr;
import org.apache.jena.sparql.expr.aggregate.Aggregator;
import org.apache.jena.sparql.expr.aggregate.AggregatorBase;
import org.apache.jena.sparql.expr.nodevalue.NodeValueBoolean;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.function.library.leviathan.LeviathanConstants;

/**
 * All aggregate, expression must evaluate to true for at least one solution
 */
public class AggAny extends AggregatorBase {

    /**
     * @param expr
     *            Expression
     */
    protected AggAny(Expr expr) {
        super(LeviathanConstants.LeviathanFunctionLibraryURI + "any", false, expr);
    }

    @Override
    public Aggregator copy(ExprList exprs) {
        return new AggAny(exprs.get(0));
    }

    @Override
    public boolean equals(Aggregator other, boolean bySyntax) {
        if (other == null)
            return false;
        if (this == other)
            return true;
        if (!(other instanceof AggAny))
            return false;
        AggAny agg = (AggAny) other;
        return getExpr().equals(agg.getExpr(), bySyntax);
    }

    @Override
    public Accumulator createAccumulator() {
        return new AccAny(this.getExpr());
    }

    @Override
    public Node getValueEmpty() {
        return NodeValue.TRUE.asNode();
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return 0;
    }

    private static class AccAny extends AccumulatorShortCircuitExpr {

        /**
         * @param expr
         */
        protected AccAny(Expr expr) {
            super(expr);
        }

        @Override
        protected NodeValue getAccValue() {
            return NodeValue.FALSE;
        }

        @Override
        protected NodeValue getErrorValue() {
            return NodeValue.FALSE;
        }

        @Override
        protected void accumulate(NodeValue nv, Binding binding, FunctionEnv functionEnv) {
            // If true can short circuit yet
            if (XSDFuncOp.booleanEffectiveValue(nv))
                this.shortCircuit(NodeValue.TRUE);
        }

        @Override
        protected void accumulateError(Binding binding, FunctionEnv functionEnv) {
            // Can't short circuit errors
        }

    }
}
