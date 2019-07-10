/*
 *
 *  * Copyright © 2017 - 2018 VMware, Inc. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the “License”); you may not use this file
 *  * except in compliance with the License. You may obtain a copy of the License at
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software distributed under the
 *  * License is distributed on an “AS IS” BASIS, without warranties or conditions of any kind,
 *  * EITHER EXPRESS OR IMPLIED. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.vrg.compiler;


import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SubqueryExpression;

import java.util.ArrayDeque;


/**
 * Extracts a stack of relevant nodes from a WHERE expression
 */
class ExpressionTraverser extends DefaultTraversalVisitor<Void, Void> {
    private final ArrayDeque<Node> stack = new ArrayDeque<>();

    @Override
    protected Void visitLogicalBinaryExpression(final LogicalBinaryExpression node, final Void context) {
        stack.push(node);
        return super.visitLogicalBinaryExpression(node, context);
    }

    @Override
    protected Void visitComparisonExpression(final ComparisonExpression node, final Void context) {
        stack.push(node);
        return super.visitComparisonExpression(node, context);
    }

    @Override
    protected Void visitArithmeticBinary(final ArithmeticBinaryExpression node, final Void context) {
        stack.push(node);
        return super.visitArithmeticBinary(node, context);
    }

    @Override
    protected Void visitArithmeticUnary(final ArithmeticUnaryExpression node, final Void context) {
        stack.push(node);
        return super.visitArithmeticUnary(node, context);
    }

    @Override
    protected Void visitExists(final ExistsPredicate node, final Void context) {
        stack.push(node);
        return super.visitExists(node, context);
    }

    @Override
    protected Void visitInPredicate(final InPredicate node, final Void context) {
        stack.push(node);
        return super.visitInPredicate(node, context);
    }

    @Override
    protected Void visitFunctionCall(final FunctionCall node, final Void context) {
        if (node.getArguments().size() == 1
            || node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
            stack.push(node);
        } else {
            throw new RuntimeException("I don't know what do with the following node: " + node);
        }
        return null;
    }

    /**
     * Parse columns like 'reference.field'
     */
    @Override
    protected Void visitDereferenceExpression(final DereferenceExpression node, final Void context) {
        stack.push(node);
        return null;
    }

    @Override
    protected Void visitSubqueryExpression(final SubqueryExpression node, final Void context) {
        stack.push(node);
        return null;
    }

    @Override
    protected Void visitLiteral(final Literal node, final Void context) {
        stack.push(node);
        return super.visitLiteral(node, context);
    }

    @Override
    protected Void visitIdentifier(final Identifier node, final Void context) {
        stack.push(node);
        return super.visitIdentifier(node, context);
    }

    @Override
    protected Void visitNotExpression(final NotExpression node, final Void context) {
        stack.push(node);
        return super.visitNotExpression(node, context);
    }

    ArrayDeque<Node> getExpressionStack() {
        return new ArrayDeque<>(stack);
    }
}
