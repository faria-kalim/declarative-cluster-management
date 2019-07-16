package com.vrg.compiler;

import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.MonoidVisitor;

import javax.annotation.Nullable;

/**
 * If a query does not have any variables in it (say, in a predicate or a join key), then they return arrays
 * of type int. If they do access variables, then they're of type "var opt" int.
 */
public class UsesControllableFields extends MonoidVisitor<Void, Void> {
    private boolean usesControllable = false;

    @Override
    protected Void visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Void context) {
        if (node.getField().isControllable()) {
            usesControllable = true;
        }
        return super.visitColumnIdentifier(node, context);
    }

    public boolean usesControllableFields() {
        return usesControllable;
    }
}
