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
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.analyzer.ResolvedField;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Created by liujing on 2024/6/25.
 */
public final class ScanAttachPredicateContext {

    private static final Logger LOG = LogManager.getLogger(ScanAttachPredicateContext.class);

    private static final ThreadLocal<ScanAttachPredicateContext>
            SCAN_ATTACH_PREDICATE_CONTEXT = new ThreadLocal<>();

    private OperatorType opType;
    private String attachTargetTablePrefix;
    private SlotRef attachCompareExpr;
    private List<LiteralExpr> attachValueExprs;

    private Scope scope;
    private ColumnRefOperator[] fieldMappings;

    public class ScanAttachPredicate {

        public ScalarOperator getOperator() {
            return null;
        }
    }

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
            LOG.info("isScanAttachPredicateTable > {} - {}", tableName, context.attachTargetTablePrefix);
            return tableName.toUpperCase().startsWith(context.attachTargetTablePrefix.toUpperCase());
        }
    }

    public static ScanAttachPredicateContext getContext() {
        return SCAN_ATTACH_PREDICATE_CONTEXT.get();
    }

    public void prepare(Scope scope, List<ColumnRefOperator> fieldMappings) {
        this.scope = scope;
        this.fieldMappings = new ColumnRefOperator[fieldMappings.size()];
        fieldMappings.toArray(this.fieldMappings);
        LOG.info("prepare -> scope: {} ", scope);
        for (int i = 0; i < this.fieldMappings.length; i++) {
            LOG.info("prepare -> fieldMappings[{}]: {}", i, this.fieldMappings[i]);
        }
        ResolvedField resolvedField = scope.resolveField(this.attachCompareExpr);
        LOG.info("prepare -> resolvedField: {}", resolvedField);
        LOG.info("prepare -> Field: {}", resolvedField != null ? resolvedField.getField() : null);
        LOG.info("prepare -> RelationFieldIndex: {}", resolvedField != null ? resolvedField.getRelationFieldIndex() : -1);
    }

    public static void beginInPredicate(SlotRef attachCompareExpr, List<LiteralExpr> attachValueExprs) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context == null) {
            context = new ScanAttachPredicateContext(OperatorType.IN);
            SCAN_ATTACH_PREDICATE_CONTEXT.set(context);
        }
        context.attachCompareExpr = attachCompareExpr;
        context.attachValueExprs = attachValueExprs;
        TableName tableName;
        Preconditions.checkNotNull(tableName = attachCompareExpr.getTblNameWithoutAnalyzed());
        context.attachTargetTablePrefix = tableName.getTbl();
    }

    public static void endInPredicate() {
        ScanAttachPredicateContext context = (ScanAttachPredicateContext) SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            context.attachCompareExpr = null;
            context.attachValueExprs = null;
            SCAN_ATTACH_PREDICATE_CONTEXT.set(null);
        }
    }
}
