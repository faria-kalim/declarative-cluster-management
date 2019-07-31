/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.esotericsoftware.reflectasm.ConstructorAccess;
import com.google.common.base.CaseFormat;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.CpSolver;
import com.google.ortools.sat.CpSolverStatus;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import com.squareup.javapoet.WildcardTypeName;
import com.vrg.IRColumn;
import com.vrg.IRContext;
import com.vrg.IRTable;
import com.vrg.compiler.monoid.BinaryOperatorPredicate;
import com.vrg.compiler.monoid.BinaryOperatorPredicateWithAggregate;
import com.vrg.compiler.monoid.ColumnIdentifier;
import com.vrg.compiler.monoid.Expr;
import com.vrg.compiler.monoid.GroupByComprehension;
import com.vrg.compiler.monoid.GroupByQualifier;
import com.vrg.compiler.monoid.JoinPredicate;
import com.vrg.compiler.monoid.MonoidComprehension;
import com.vrg.compiler.monoid.MonoidFunction;
import com.vrg.compiler.monoid.MonoidLiteral;
import com.vrg.compiler.monoid.MonoidVisitor;
import com.vrg.compiler.monoid.TableRowGenerator;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record3;
import org.jooq.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.lang.model.element.Modifier;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * This class generates Java code that invokes the or-tools CP-SAT solver. The generated
 * class is compiled, and loaded via ReflectASM to return an IGeneratedBackend instance,
 * which can be invoked to run the solver.
 */
public class OrToolsSolver implements ISolverBackend {
    private static final Logger LOG = LoggerFactory.getLogger(OrToolsSolver.class);
    private static final String generatedBackendName = "GeneratedBackend";
    private static final String generatedFieldNamePrefix = "GenField";
    private static final MethodSpec intVarNoBounds = MethodSpec.methodBuilder("intVarNoBounds")
                                                .addModifiers(Modifier.PRIVATE)
                                                .addParameter(CpModel.class, "model", Modifier.FINAL)
                                                .addParameter(String.class, "name",  Modifier.FINAL)
                                                .returns(IntVar.class)
                                                .addStatement("return model.newIntVar(0, Integer.MAX_VALUE - 1, name)")
                                                .build();
    private final Map<String, Map<String, Integer>> viewToFieldIndex = new HashMap<>();
    private final AtomicInteger generatedFieldNameCounter = new AtomicInteger();
    private final AtomicInteger intermediateViewCounter = new AtomicInteger();
    private final Map<String, Map<String, String>> tableToFieldToType = new HashMap<>();
    private final Map<String, String> viewTupleTypeParameters = new HashMap<>();
    private final Map<String, String> viewGroupByTupleTypeParameters = new HashMap<>();

    static {
        System.loadLibrary("jniortools");
    }

    @Nullable private IGeneratedBackend generatedBackend = null;
    @Nullable private IRContext context = null;

    @Override
    public Map<IRTable, Result<? extends Record>> runSolver(final DSLContext dbCtx,
                                                            final Map<String, IRTable> irTables) {
        Preconditions.checkNotNull(generatedBackend);
        Preconditions.checkNotNull(context);
        return generatedBackend.solve(context);
    }

    /**
     * This method is where the code generation happens.
     *
     * It generates an instance of IGeneratedBackend with a solve() method. The solve method
     * creates all the IntVar instances required for variable columns, and translates comprehensions
     * into nested for loops. It re-uses JOOQ tables wherever possible for constants.
     */
    @Override
    public List<String> generateModelCode(final IRContext context,
                                          final Map<String, MonoidComprehension> nonConstraintViews,
                                          final Map<String, MonoidComprehension> constraintViews,
                                          final Map<String, MonoidComprehension> objectiveFunctions) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("solve");
        addInitializer(builder);
        try {
            addArrayDeclarations(builder, context);
        } catch (final ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        nonConstraintViews
                .forEach((name, comprehension) -> {
                    final MonoidComprehension rewrittenComprehension = rewritePipeline(comprehension);
                    addNonConstraintView(builder, name, rewrittenComprehension, context);
                });
        addSolvePhase(builder, context);
        final MethodSpec solveMethod = builder.addStatement("return null").build();

        final TypeSpec.Builder backendClassBuilder = TypeSpec.classBuilder(generatedBackendName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addSuperinterface(IGeneratedBackend.class)
                .addMethod(solveMethod)
                .addMethod(intVarNoBounds);
        TupleGen.getAllTupleTypes().forEach(backendClassBuilder::addType); // Add tuple types

        final TypeSpec spec = backendClassBuilder.build();
        return compile(spec);
    }


    private void addNonConstraintView(final MethodSpec.Builder builder, final String viewName,
                                      final MonoidComprehension comprehension, final IRContext context) {
        if (comprehension instanceof GroupByComprehension) {
            final GroupByComprehension groupByComprehension = (GroupByComprehension) comprehension;
            final MonoidComprehension inner = groupByComprehension.getComprehension();
            final GroupByQualifier groupByQualifier = groupByComprehension.getGroupByQualifier();

            // Rewrite inner comprehension using the following rule:
            // [ [blah - sum(X) | <....>] | G]
            //                     rows
            //

            // We create an intermediate view that extracts groups and returns a List<Tuple> per group
            final String intermediateView = "tmp" + intermediateViewCounter.getAndIncrement();
            buildInnerComprehension(builder, intermediateView, inner, groupByQualifier);

            // We now construct the actual result set that hosts the aggregated tuples by group. This is done
            // in two steps...
            final int groupByQualifiersSize = groupByQualifier.getColumnIdentifiers().size();
            final int selectExprSize = inner.getHead().getSelectExprs().size();
            final String groupByTupleTypeParameters = viewGroupByTupleTypeParameters.get(intermediateView);
            final String headItemsTupleTypeParamters = viewTupleTypeParameters.get(intermediateView);
            assert inner.getHead() != null;
            final int innerTupleSize = viewToFieldIndex.get(intermediateView.toUpperCase(Locale.US)).size();

            // (1) Create the result set
            builder.addCode("\n");
            builder.addComment("Non-constraint view $L", tableNameStr(viewName));
            final TypeSpec typeSpec = TupleGen.getTupleType(selectExprSize);
            final String viewTupleGenericParameters = generateTupleGenericParameters(inner.getHead().getSelectExprs());
            viewTupleTypeParameters.put(tableNameStr(viewName), viewTupleGenericParameters);
            builder.addStatement("final $T<$N<$L>> $L = new $T<>($L.size())", List.class, typeSpec,
                                  viewTupleGenericParameters, tableNameStr(viewName), ArrayList.class, intermediateView);

            // (2) Populate the result set
            builder.beginControlFlow("for (final $T<Tuple$L<$L>, List<Tuple$L<$L>>> entry: $L.entrySet())",
                                     Map.Entry.class, groupByQualifiersSize, groupByTupleTypeParameters, innerTupleSize,
                                     headItemsTupleTypeParamters, intermediateView);
            builder.addStatement("final Tuple$L<$L> group = entry.getKey()", groupByQualifiersSize,
                                                                             groupByTupleTypeParameters);
            builder.addStatement("final List<Tuple$L<$L>> data = entry.getValue()", innerTupleSize,
                                                                                   headItemsTupleTypeParamters);
            builder.addCode("final $1N<$2L> res = new $1N<>(", typeSpec, viewTupleGenericParameters);
            inner.getHead().getSelectExprs()
                    .forEach(
                        e -> {
                            final String result =
                                    exprToStr(e, true, new GroupContext(groupByQualifier, intermediateView));
                            if (result.contains("$T")) {
                                builder.addCode(result, Collectors.class);
                            } else {
                                builder.addCode(result);
                            }
                        }
                    );
            builder.addCode(");\n");
            builder.endControlFlow();
        } else {
            buildInnerComprehension(builder, viewName, comprehension, null);
        }

    }

    /**
     * Converts a comprehension into a set of nested for loops that return a "result set". The result set
     * is represented as a list of tuples.
     */
    private void buildInnerComprehension(final MethodSpec.Builder builder, final String viewName,
                                         final MonoidComprehension comprehension,
                                         @Nullable final GroupByQualifier groupByQualifier) {
        Preconditions.checkNotNull(comprehension.getHead());
        builder.addCode("\n").addComment("Non-constraint view $L", viewName);

        final List<BinaryOperatorPredicate> wherePredicates = new ArrayList<>();
        final List<JoinPredicate> joinPredicates = new ArrayList<>();
        final List<BinaryOperatorPredicateWithAggregate> aggregatePredicates = new ArrayList<>();
        final List<TableRowGenerator> tableRowGenerators = new ArrayList<>();
        comprehension.getQualifiers().forEach(
                q -> {
                    if (q instanceof BinaryOperatorPredicateWithAggregate) {
                        aggregatePredicates.add((BinaryOperatorPredicateWithAggregate) q);
                    } else if (q instanceof JoinPredicate) {
                        joinPredicates.add((JoinPredicate) q);
                    } else if (q instanceof BinaryOperatorPredicate) {
                        wherePredicates.add((BinaryOperatorPredicate) q);
                    } else if (q instanceof TableRowGenerator) {
                        tableRowGenerators.add((TableRowGenerator) q);
                    } else if (q instanceof GroupByQualifier) {
                        System.err.println("Ignoring group-by qualifier");
                    } else {
                        throw new IllegalArgumentException();
                    }
                }
        );
        Preconditions.checkArgument(aggregatePredicates.isEmpty()); // Temporary

        // Initialize an arraylist or map to collect results and generate the required tuple sizes
        final AtomicInteger fieldIndex = new AtomicInteger();
        final List<ColumnIdentifier> headItemsList = comprehension.getHead().getSelectExprs().stream()
                .map(this::getColumnsAccessed)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        final String headItemsStr = headItemsList.stream()
                .map(expr -> convertToFieldAccess(expr, viewName, fieldIndex))
                .collect(Collectors.joining(",\n    "));
        final String headItemsListTupleGenericParameters = generateTupleGenericParameters(headItemsList);
        viewTupleTypeParameters.put(viewName, headItemsListTupleGenericParameters);

        final int tupleSize = headItemsList.size();
        // Generates a tuple type if needed
        final TypeSpec tupleSpec = TupleGen.getTupleType(tupleSize);

        final String viewRecords = nonConstraintViewName(viewName);

        if (groupByQualifier != null) {
            // Create group by tuple
            final int numberOfGroupColumns = groupByQualifier.getColumnIdentifiers().size();
            final String groupByTupleGenericParameters =
                    generateTupleGenericParameters(groupByQualifier.getColumnIdentifiers());
            final TypeSpec groupTupleSpec = TupleGen.getTupleType(numberOfGroupColumns);
            viewGroupByTupleTypeParameters.put(viewName, groupByTupleGenericParameters);
            builder.addStatement("final Map<$N<$L>, $T<$N<$L>>> $L = new $T<>()",
                                 groupTupleSpec, groupByTupleGenericParameters,
                                 List.class, tupleSpec, headItemsListTupleGenericParameters,
                                 viewRecords, HashMap.class);
        } else {
            builder.addStatement("final $T<$N<$L>> $L = new $T<>()",
                    List.class, tupleSpec, headItemsListTupleGenericParameters, viewRecords, ArrayList.class);
        }

        // Create nested for loops
        tableRowGenerators.forEach(tr -> {
            final String tableName = tr.getTable().getName();
            final String tableNumRowsStr = tableNumRowsStr(tableName);
            final String iterStr = iterStr(tr.getTable().getAliasedName());
            builder.beginControlFlow("for (int $1L = 0; $1L < $2L; $1L++)", iterStr, tableNumRowsStr);
        });

        final String joinPredicateStr = joinPredicates.stream()
                .map(expr -> exprToStr(expr, false, null))
                .collect(Collectors.joining(" \n    && "));
        final String wherePredicateStr = wherePredicates.stream()
                .map(expr -> exprToStr(expr, false, null))
                .collect(Collectors.joining(" \n    && "));
        final String predicateStr = Stream.of(joinPredicateStr, wherePredicateStr)
                .filter(s -> !s.equals(""))
                .collect(Collectors.joining(" \n    && "));
        // Add filter predicate
        builder.beginControlFlow("if ($L)", predicateStr);


        // If predicate is true, then retrieve expressions to collect into result set. Note, this does not
        // evaluate things like functions (sum etc.). These are not aggregated as part of the inner expressions.

        // Create a tuple for the result set
        builder.addStatement("final Tuple$1L<$2L> tuple = new Tuple$1L<>(\n    $3L\n    )",
                             tupleSize, headItemsListTupleGenericParameters, headItemsStr);

        // Update result set
        if (groupByQualifier != null) {
            final int numberOfGroupByColumns = groupByQualifier.getColumnIdentifiers().size();
            // Comma separated list of field accesses to construct a group string
            final String groupString = groupByQualifier.getColumnIdentifiers().stream()
                    .map(e -> fieldNameStrWithIter(e.getTableName(), e.getField().getName()))
                    .collect(Collectors.joining(",     \n"));

            // Organize the collected tuples from the nested for loops by groupByTuple
            builder.addStatement("final Tuple$1L<$2L> groupByTuple = new Tuple$1L<>(\n    $3L\n    )",
                                 numberOfGroupByColumns, viewGroupByTupleTypeParameters.get(viewName), groupString);
            builder.addStatement("$L.computeIfAbsent(groupByTuple, (k) -> new $T<>()).add(tuple)",
                                 viewRecords, ArrayList.class);
        } else {
            builder.addStatement("$L.add(tuple)", viewRecords);
        }
        builder.endControlFlow(); // end filter predicate if statement

        // Pop nested for loops
        tableRowGenerators.forEach(tr -> builder.endControlFlow());

        // Print debugging info
        // builder.addStatement("$T.out.println($N)", System.class, viewRecords);
    }

    private LinkedHashSet<ColumnIdentifier> getColumnsAccessed(final Expr expr) {
        final GetColumnIdentifiers visitor = new GetColumnIdentifiers();
        visitor.visit(expr);
        return visitor.getColumnIdentifiers();
    }

    private String convertToFieldAccess(final Expr expr, final String viewName, final AtomicInteger fieldIndex) {
        Preconditions.checkArgument(expr instanceof ColumnIdentifier);
        final String fieldName = updateFieldIndex(viewName, expr, fieldIndex);
        return exprToStr(expr)  + " /* " + fieldName + " */";
    }

    private String updateFieldIndex(final String viewName, final Expr argument, final AtomicInteger counter) {
        final String fieldName = argument.getAlias().orElseGet(() -> {
                        if (argument instanceof ColumnIdentifier) {
                            return ((ColumnIdentifier) argument).getField().getName();
                        } else {
                            return generatedFieldNamePrefix + generatedFieldNameCounter.getAndIncrement();
                        }
                    }
                )
                .toUpperCase(Locale.US);
        viewToFieldIndex.computeIfAbsent(viewName.toUpperCase(Locale.US), (k) -> new HashMap<>())
                        .computeIfAbsent(fieldName, (k) -> counter.getAndIncrement());
        return fieldName;
    }

    /**
     * Creates array declarations and returns the set of tables that have controllable columns in them.
     */
    private void addArrayDeclarations(final MethodSpec.Builder builder, final IRContext context) throws ClassNotFoundException {
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            // For each table...
            builder.addComment("Table $S", table.getName());

            // ...1) figure out the jooq record type (e.g., Record3<Integer, String, Boolean>)
            final Class recordType = Class.forName("org.jooq.Record" + table.getIRColumns().size());
            final String recordTypeParameters = table.getIRColumns().entrySet().stream()
                    .map(e -> {
                        final String retVal = InferType.typeStringFromColumn(e.getValue());
                        // Tracks the type of each field.
                        tableToFieldToType.computeIfAbsent(table.getName(), (k) -> new HashMap<>())
                                          .putIfAbsent(e.getKey(), retVal);
                        return retVal;
                    }
                    ).collect(Collectors.joining(", "));
            // ...2) create a List<[RecordType]> to represent the table
            builder.addStatement("final $T<$T<$L>> $L = (List<$T<$L>>) context.getTable($S).getCurrentData()",
                                 List.class, recordType, recordTypeParameters, tableNameStr(table.getName()),
                                 recordType, recordTypeParameters, table.getName());

            // ...3) for controllable fields, create a corresponding array of IntVars.
            for (final Map.Entry<String, IRColumn> fieldEntrySet : table.getIRColumns().entrySet()) {
                final String fieldName = fieldEntrySet.getKey();
                final IRColumn field = fieldEntrySet.getValue();
                if (field.isControllable()) {
                    final String variableName = fieldNameStr(table.getName(), fieldName);
                    builder.addStatement("final $T[] $L = new $T[$L]", IntVar.class, variableName, IntVar.class,
                                                                       tableNumRowsStr(table.getName()))
                            .beginControlFlow("for (int i = 0; i < $L; i++)",
                                              tableNumRowsStr(table.getName()))
                            .addStatement("$L[i] = $N(model, $S)", variableName, intVarNoBounds, fieldName)
                            .endControlFlow();
                }
            }
        }
    }

    private void addInitializer(final MethodSpec.Builder builder) {
        // ? extends Record
        final WildcardTypeName recordT = WildcardTypeName.subtypeOf(Record.class);
        // Result<? extends Record>
        final ParameterizedTypeName resultT = ParameterizedTypeName.get(ClassName.get(Result.class), recordT);
        final ClassName irTableT = ClassName.get(IRTable.class);
        final ClassName map = ClassName.get(Map.class);
        // Map<IRTable, Result<? extends Record>>
        final ParameterizedTypeName returnT = ParameterizedTypeName.get(map, irTableT, resultT);
        builder.addModifiers(Modifier.PUBLIC)
               .returns(returnT)
               .addParameter(IRContext.class, "context")
               .addComment("Create the model.")
               .addStatement("final $T model = new $T()", CpModel.class, CpModel.class)
               .addCode("\n");
    }

    private void addSolvePhase(final MethodSpec.Builder builder, final IRContext context) {
        builder.addCode("\n")
               .addComment("Start solving")
               .addStatement("final $1T solver = new $1T()", CpSolver.class)
               .addStatement("final $T status = solver.solve(model)", CpSolverStatus.class)
               .beginControlFlow("if (status == CpSolverStatus.FEASIBLE || status == CpSolverStatus.OPTIMAL)")
               .addStatement("final Map<IRTable, Result<? extends Record>> result = new $T<>()",
                              HashMap.class);
        for (final IRTable table: context.getTables()) {
            if (table.isViewTable() || table.isAliasedTable()) {
                continue;
            }
            final String tableName = table.getName();
            final Set<String> controllableColumns = table.getIRColumns().entrySet().stream()
                                                                   .filter(e -> e.getValue() // IRColumn
                                                                                 .isControllable())
                                                                   .map(Map.Entry::getKey)
                                                                   .collect(Collectors.toSet());
            // If the table does not have vars, we can return the original input as is
            if (controllableColumns.isEmpty()) {
                builder.addStatement("result.put(context.getTable($1S), context.getTable($1S).getCurrentData())",
                                      tableName);
            } else {
                table.getIRColumns().forEach(
                    (name, field) -> {
                        // Else, we update the records corresponding to the table.
                        if (controllableColumns.contains(name)) {

                        }
                    }
                );
            }
        }
        builder.addStatement("return result");
        builder.endControlFlow();
    }

    private static String tableNameStr(final String tableName) {
        return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName);
    }

    private static String fieldNameStr(final String tableName, final String fieldName) {
        return String.format("%s%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName),
                                     CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, fieldName));
    }

    private String fieldNameStrWithIter(final String tableName, final String fieldName) {
        if (fieldName.contains("CONTROLLABLE")) {
            return String.format("%s[%s]", fieldNameStr(tableName, fieldName), iterStr(tableName));
        } else {
            if (viewToFieldIndex.containsKey(tableName)) {
                final int fieldIndex = viewToFieldIndex.get(tableName).get(fieldName);
                return String.format("%s.get(%s).value%s()", tableNameStr(tableName), iterStr(tableName), fieldIndex);
            } else {
                final String type = tableToFieldToType.get(tableName).get(fieldName);
                return String.format("%s.get(%s).get(\"%s\", %s.class)",
                        tableNameStr(tableName), iterStr(tableName), fieldName, type);
            }
        }
    }

    private static String tableNumRowsStr(final String tableName) {
        return String.format("%s.size()", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String iterStr(final String tableName) {
        return String.format("%sIter", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private static String nonConstraintViewName(final String tableName) {
        return String.format("%s", CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, tableName));
    }

    private List<String> compile(final TypeSpec spec) {
        final JavaFile javaFile = JavaFile.builder("com.vrg.backend", spec).build();
        LOG.info("Generating Java or-tools code: {}\n", javaFile.toString());

        // Compile Java code. This steps requires an SDK, and a JRE will not suffice
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final StandardJavaFileManager fileManager =
                compiler.getStandardFileManager(null, null, null);
        final Iterable<? extends JavaFileObject> compilationUnit = Collections.singleton(javaFile.toJavaFileObject());

        try {
            // Invoke SDK compiler
            final Path path = Path.of("/tmp/compilerOutput");
            final BufferedWriter fileWriter = Files.newBufferedWriter(path);
            final Boolean call = compiler.getTask(fileWriter, fileManager, null,
                    ImmutableList.of("-verbose", "-d", "/tmp/"), null, compilationUnit)
                    .call();
            if (!call) {
                final List<String> strings = Files.readAllLines(path);
                LOG.error("Compilation failed");
                for (final String line: strings) {
                    LOG.error("{}", line);
                }
                throw new RuntimeException();
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        // Once compiled, load the generated class, save an instance of it to the generatedBackend method
        // which we will invoke whenever we run the solver, and return the generated Java source to the caller.
        try {
            File classesDir = new File("/tmp/");
            // Load and instantiate compiled class.
            URLClassLoader classLoader;
            // Loading the class
            classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()});
            Class<?> cls;
            cls = Class.forName(String.format("com.vrg.backend.%s", generatedBackendName), true, classLoader);
            ConstructorAccess<?> access = ConstructorAccess.get(cls);
            generatedBackend = (IGeneratedBackend) access.newInstance();
            final StringWriter writer = new StringWriter();
            javaFile.writeTo(writer);
            return Collections.singletonList(writer.toString());
        } catch (final IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> generateDataCode(final IRContext context) {
        this.context = context;
        return null;
    }
    private String exprToStr(final Expr expr) {
        return exprToStr(expr, true, null);
    }

    private String exprToStr(final Expr expr, final boolean allowControllable,
                             @Nullable final GroupContext currentGroup) {
        final ExprToStrVisitor visitor = new ExprToStrVisitor(allowControllable, currentGroup);
        return Objects.requireNonNull(visitor.visit(expr, false));
    }

    private MonoidComprehension rewritePipeline(final MonoidComprehension comprehension) {
        return RewriteArity.apply(RewriteCountFunction.apply(comprehension));
    }

    private <T extends Expr> String generateTupleGenericParameters(final List<T> exprs) {
        return exprs.stream().map(this::generateTupleGenericParameters)
                             .collect(Collectors.joining(", "));
    }

    private String generateTupleGenericParameters(final Expr expr) {
        final InferType visitor = new InferType();
        final String result = visitor.visit(expr);
        return Objects.requireNonNull(result);
    }


    private class ExprToStrVisitor extends MonoidVisitor<String, Boolean> {
        private final boolean allowControllable;
        @Nullable private final GroupContext currentGroupContext;

        private ExprToStrVisitor(final boolean allowControllable, @Nullable final GroupContext currentGroupContext) {
            this.allowControllable = allowControllable;
            this.currentGroupContext = currentGroupContext;
        }

        @Nullable
        @Override
        protected String visitMonoidFunction(final MonoidFunction node, @Nullable final Boolean isFunctionContext) {
            final String processedArgument = visit(node.getArgument(), true);
            final String functionName = node.getFunctionName();
            final String ret = String.format("%s(data.stream()\n        .map(t -> %s)\n        .collect($T.toList()))",
                                              functionName, processedArgument);
            System.out.println(ret);
            System.out.println(processedArgument);
            return ret;
        }

        @Nullable
        @Override
        protected String visitBinaryOperatorPredicate(final BinaryOperatorPredicate node,
                                                      @Nullable final Boolean isFunctionContext) {
            final String left = visit(node.getLeft(), isFunctionContext);
            final String right = visit(node.getRight(), isFunctionContext);
            final String op = node.getOperator();
            switch (op) {
                case "==":
                    return String.format("eq(%s, %s)", left, right);
                case "!=":
                    return String.format("ne(%s, %s)", left, right);
                case "/\\":
                    return String.format("and(%s, %s)", left, right);
                case "\\/":
                    return String.format("or(%s, %s)", left, right);
                case "<=":
                    return String.format("leq(%s, %s)", left, right);
                case "<":
                    return String.format("le(%s, %s)", left, right);
                case ">=":
                    return String.format("geq(%s, %s)", left, right);
                case ">":
                    return String.format("gt(%s, %s)", left, right);
                case "+":
                    return String.format("plus(%s, %s)", left, right);
                case "-":
                    return String.format("minus(%s, %s)", left, right);
                case "*":
                    return String.format("mult(%s, %s)", left, right);
                case "/":
                    return String.format("div(%s, %s)", left, right);
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Nullable
        @Override
        protected String visitColumnIdentifier(final ColumnIdentifier node, @Nullable final Boolean isFunctionContext) {
            Preconditions.checkNotNull(isFunctionContext);
            // If we are evaluating a group-by comprehension, then column accesses that happen outside the context
            // of an aggregation function must refer to the grouping column, not the inner tuple being iterated over.
            if (!isFunctionContext && currentGroupContext != null) {
                final String tableName = node.getTableName();
                final String fieldName = node.getField().getName();
                int i = 0;
                for (final ColumnIdentifier ci: currentGroupContext.qualifier.getColumnIdentifiers()) {
                    if (ci.getTableName().equalsIgnoreCase(tableName)
                            && ci.getField().getName().equalsIgnoreCase(fieldName)) {
                        return String.format("group.value%s()", i);
                    }
                    i++;
                }
                throw new UnsupportedOperationException("Could not find group-by column");
            }

            if (isFunctionContext && currentGroupContext != null) {
                final String tempTableName = currentGroupContext.groupViewName.toUpperCase(Locale.US);
                final int fieldIndex = viewToFieldIndex.get(tempTableName).get(node.getField().getName());
                return String.format("t.value%s()", fieldIndex);
            }

            final String tableName = node.getTableName();
            final String fieldName = node.getField().getName();
            if (!allowControllable && fieldName.contains("CONTROLLABLE")) {
                throw new UnsupportedOperationException("Controllable variables not allowed in predicates");
            }
            return fieldNameStrWithIter(tableName, fieldName);
        }

        @Nullable
        @Override
        protected String visitMonoidLiteral(final MonoidLiteral node, @Nullable final Boolean isFunctionContext) {
            if (node.getValue() instanceof String) {
                return node.getValue().toString().replace("'", "\"");
            } else {
                return node.getValue().toString();
            }
        }
    }

    private static class GroupContext {
        private final GroupByQualifier qualifier;
        private final String groupViewName;

        private GroupContext(final GroupByQualifier qualifier, final String groupViewName) {
            this.qualifier = qualifier;
            this.groupViewName = groupViewName;
        }
    }
}