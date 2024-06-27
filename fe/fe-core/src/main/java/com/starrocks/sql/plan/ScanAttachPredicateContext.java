// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2024/6/25.
 */
public final class ScanAttachPredicateContext {

    private static final Logger LOG = LogManager.getLogger(ScanAttachPredicateContext.class);

    private static final ThreadLocal<ScanAttachPredicateContext>
            SCAN_ATTACH_PREDICATE_CONTEXT = new ThreadLocal<>();

    private final OperatorType opType;
    private String attachTargetTablePrefix;
    private SlotRef attachCompareExpr;
    private LiteralExpr[] attachValueExprs;

    private int relationFieldIndex;
    private ColumnRefOperator[] fieldMappings;
    private Column[] columnMappings;
    ScalarOperator[] scalarOperators;

    private ScanAttachPredicateContext(OperatorType opType) {
        this.opType = opType;
    }

    public OperatorType getOpType() {
        return opType;
    }

    public static boolean isScanAttachPredicateTable(String tableName) {
        ScanAttachPredicateContext context = getContext();
        if (context == null || tableName == null) {
            return false;
        } else {
            return tableName.toUpperCase().startsWith(context.attachTargetTablePrefix.toUpperCase());
        }
    }

    public static ScanAttachPredicateContext getContext() {
        return SCAN_ATTACH_PREDICATE_CONTEXT.get();
    }

    public void prepare(Scope scope,
                        List<ColumnRefOperator> fieldMappings,
                        Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        this.columnMappings = new Column[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap =
                columnMetaToColRefMap
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        for (int i = 0; i < this.fieldMappings.length; i++) {
            columnMappings[i] = colRefToColumnMetaMap.get(this.fieldMappings[i]);
            if (this.fieldMappings[i].getName().equals(this.attachCompareExpr.getColumnName())) {
                this.relationFieldIndex = i;
                LOG.info("ScanAttachPredicateContext prepare, " +
                        "relationFieldIndex: {}.", this.relationFieldIndex);
            }
        }
        ResolvedField resolvedField;
        try {
            resolvedField = scope.resolveField(this.attachCompareExpr);
            this.relationFieldIndex = resolvedField.getRelationFieldIndex();
        } catch (Exception exception) {
            resolvedField = null;
            LOG.error("prepare -> resolveField ", exception);
        }
        this.scalarOperators = new ScalarOperator[attachValueExprs.length + 1];
        scalarOperators[0] = this.fieldMappings[this.relationFieldIndex];
        for (int i = 0; i < attachValueExprs.length; i++) {
            scalarOperators[i + 1] = visitLiteral(attachValueExprs[i]);
        }
        LOG.info("ScanAttachPredicateContext prepare, " +
                        "scalarOperators: {}, relationFieldIndex: {}.",
                scalarOperators != null ? Arrays.toString(scalarOperators) : null,
                this.relationFieldIndex);
    }

    public Column getAttachColumn() {
        return this.columnMappings[this.relationFieldIndex];
    }

    public ColumnRefOperator getAttachColumnRefOperator() {
        return this.fieldMappings[this.relationFieldIndex];
    }

    public ScalarOperator getAttachPredicate() {
        return new InPredicateOperator(false, scalarOperators);
    }

    protected ScalarOperator visitLiteral(LiteralExpr node) {
        if (node instanceof NullLiteral) {
            return ConstantOperator.createNull(node.getType());
        }
        return ConstantOperator.createObject(node.getRealObjectValue(), node.getType());
    }

    public static void beginInPredicate(SlotRef attachCompareExpr, List<LiteralExpr> attachValueExprs) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context == null) {
            context = new ScanAttachPredicateContext(OperatorType.IN);
            SCAN_ATTACH_PREDICATE_CONTEXT.set(context);
        }
        context.attachCompareExpr = attachCompareExpr;
        context.attachValueExprs = new LiteralExpr[attachValueExprs.size()];
        attachValueExprs.toArray(context.attachValueExprs);
        TableName tableName;
        Preconditions.checkNotNull(tableName = attachCompareExpr.getTblNameWithoutAnalyzed());
        context.attachTargetTablePrefix = tableName.getTbl();
    }

    public static void endInPredicate() {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            context.attachTargetTablePrefix = null;
            context.attachCompareExpr = null;
            context.attachValueExprs = null;

            context.fieldMappings = null;
            context.columnMappings = null;
            context.scalarOperators = null;
            SCAN_ATTACH_PREDICATE_CONTEXT.set(null);
        }
    }
}
