package com.vrg.backend;

import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.ComprehensionRewriter;
import com.vrg.compiler.monoid.Expr;
import com.vrg.compiler.monoid.MonoidComprehension;
import com.vrg.compiler.monoid.MonoidFunction;
import com.vrg.compiler.monoid.MonoidLiteral;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Minizinc has no "count()" function. We rewrite all instances of
 * count([i | qualifiers..]) to sum([1 | qualifiers...]).
 */
class RewriteCountFunction {

    static MonoidComprehension apply(final MonoidComprehension comprehension) {
        final CountRewriter rewriter = new CountRewriter();
        final MonoidComprehension newCompr =
                (MonoidComprehension) Objects.requireNonNull(rewriter.visit(comprehension));
        return newCompr;
    }

    static class CountRewriter extends ComprehensionRewriter<Void> {
        @Override
        protected Expr visitMonoidFunction(final MonoidFunction function, @Nullable final Void context) {
            if (function.getFunctionName().equalsIgnoreCase("count")) {
                if (!(function.getArgument() instanceof ColumnIdentifier)) {
                    throw new IllegalStateException("RewriteCountFunction is only safe to use on column identifiers");
                }
                final MonoidFunction newFunction = new MonoidFunction("sum", new MonoidLiteral<>(1));
                function.getAlias().ifPresent(newFunction::setAlias);
                return newFunction;
            }
            return super.visitMonoidFunction(function, context);
        }
    }
}
