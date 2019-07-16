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

package com.vrg.compiler.monoid;

import javax.annotation.Nullable;

public class MonoidVisitor<T, C> {

    @Nullable
    public T visit(final Expr expr, @Nullable final C context) {
        return expr.acceptVisitor(this, context);
    }

    @Nullable
    public T visit(final Expr expr) {
        return visit(expr, null);
    }

    @Nullable
    protected T visitHead(final Head node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitTableRowGenerator(final TableRowGenerator node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidComprehension(final MonoidComprehension node, @Nullable final C context) {
        node.getHead().acceptVisitor(this, context);
        for (final Qualifier qualifier: node.getQualifiers()) {
            qualifier.acceptVisitor(this, context);
        }
        return null;
    }

    @Nullable
    protected T visitBinaryOperatorPredicate(final BinaryOperatorPredicate node, @Nullable final C context) {
        node.getLeft().acceptVisitor(this, context);
        node.getRight().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitGroupByComprehension(final GroupByComprehension node, @Nullable final C context) {
        node.getComprehension().acceptVisitor(this, context);
        node.getGroupByQualifier().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitGroupByQualifier(final GroupByQualifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidLiteral(final MonoidLiteral node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitMonoidFunction(final MonoidFunction node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }

    @Nullable
    protected T visitQualifier(final Qualifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitColumnIdentifier(final ColumnIdentifier node, @Nullable final C context) {
        return null;
    }

    @Nullable
    protected T visitExistsPredicate(final ExistsPredicate node, @Nullable final C context) {
        node.getArgument().acceptVisitor(this, context);
        return null;
    }
}