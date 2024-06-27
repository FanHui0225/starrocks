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
import com.starrocks.sql.ast.TableRelation;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by liujing on 2024/6/25.
 */
public final class ScanAttachPredicateContext {

    private static final Logger LOG = LogManager.getLogger(ScanAttachPredicateContext.class);

    private static final ThreadLocal<ScanAttachPredicateContext>
            SCAN_ATTACH_PREDICATE_CONTEXT = new ThreadLocal<>();

    private final OperatorType opType;

    public class ScanAttachPredicate {
        String tableName;
        String columnName;
        SlotRef attachCompareExpr;
        LiteralExpr[] attachValueExprs;

        int relationFieldIndex;
        ColumnRefOperator[] fieldMappings;
        Column[] columnMappings;
        ScalarOperator[] scalarOperators;

        ScanAttachPredicate(TableName tableName, SlotRef attachCompareExpr, LiteralExpr[] attachValueExprs) {
            this.attachCompareExpr = attachCompareExpr;
            this.attachValueExprs = new LiteralExpr[attachValueExprs.length];
            System.arraycopy(attachValueExprs, 0, this.attachValueExprs, 0, attachValueExprs.length);
            this.tableName = tableName.getNoClusterString();
            this.columnName = attachCompareExpr.getColumnName();
        }

        void resolve(Scope scope,
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
                this.columnMappings[i] = colRefToColumnMetaMap.get(this.fieldMappings[i]);
                if (this.fieldMappings[i].getName().equals(this.columnName)) {
                    this.relationFieldIndex = i;
                    LOG.info("ScanAttachPredicate[{}]-[{}] resolve, relationFieldIndex: {}.",
                            this.tableName,
                            this.columnName,
                            this.relationFieldIndex);
                }
            }
            ResolvedField resolvedField;
            try {
                resolvedField = scope.resolveField(this.attachCompareExpr);
                this.relationFieldIndex = resolvedField.getRelationFieldIndex();
            } catch (Exception ex) {
                resolvedField = null;
                LOG.error("ScanAttachPredicate[{}]-[{}] resolve field error,",
                        this.tableName,
                        this.columnName,
                        ex);
            }
            this.scalarOperators = new ScalarOperator[attachValueExprs.length + 1];
            this.scalarOperators[0] = this.fieldMappings[this.relationFieldIndex];
            for (int i = 0; i < attachValueExprs.length; i++) {
                this.scalarOperators[i + 1] = visitLiteral(attachValueExprs[i]);
            }
            LOG.info("ScanAttachPredicate[{}]-[{}] resolve, scalarOperators: {}, relationFieldIndex: {}.",
                    this.tableName,
                    this.columnName,
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

        public String getAttachTableName() {
            return this.tableName;
        }

        ScalarOperator visitLiteral(LiteralExpr node) {
            if (node instanceof NullLiteral) {
                return ConstantOperator.createNull(node.getType());
            }
            return ConstantOperator.createObject(node.getRealObjectValue(), node.getType());
        }
    }

    static class SlotRefMatcher implements Predicate<TableName> {

        SlotRef attachCompareExpr;

        SlotRefMatcher(SlotRef attachCompareExpr) {
            this.attachCompareExpr = attachCompareExpr;
        }

        @Override
        public boolean test(TableName tableName) {
            String dbName = tableName.getDb();
            String tblName = tableName.getTbl();
            TableName testTableName = attachCompareExpr.getTableName();
            String testDbName = testTableName.getDb();
            String testTblName = testTableName.getTbl();

            LOG.info("SlotRefMatcher test, " +
                            "dbName: {}, " +
                            "tblName: {}, " +
                            "testDbName: {}, " +
                            "testTblName: {}.",
                    dbName,
                    tblName,
                    testDbName,
                    testTblName);
            if (testDbName == null) {
                return tblName.startsWith(testTblName);
            } else {
                return dbName.equals(testDbName) && tblName.startsWith(testTblName);
            }
        }
    }

    private LiteralExpr[] attachValueExprs;
    private SlotRefMatcher[] slotRefMatchers;
    // raw db table id -> predicate
    private Map<Long, ScanAttachPredicate> tblIdToAttachPredicateMap = new ConcurrentHashMap<>();

    private ScanAttachPredicateContext(OperatorType opType,
                                       SlotRef[] attachCompareExprs,
                                       LiteralExpr[] attachValueExprs) {
        this.opType = opType;
        this.slotRefMatchers = new SlotRefMatcher[attachCompareExprs.length];
        for (int i = 0; i < slotRefMatchers.length; i++) {
            SlotRef attachCompareExpr = attachCompareExprs[i];
            this.slotRefMatchers[i] = new SlotRefMatcher(attachCompareExpr);
        }
        this.attachValueExprs = attachValueExprs;
    }

    public OperatorType getOpType() {
        return opType;
    }

    public void prepare(long tableId,
                        TableName tableName,
                        Scope scope,
                        List<ColumnRefOperator> fieldMappings,
                        Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        Preconditions.checkNotNull(tableName);
        for (SlotRefMatcher matcher : slotRefMatchers) {
            if (matcher.test(tableName)) {
                ScanAttachPredicate predicate = new ScanAttachPredicate(
                        tableName,
                        matcher.attachCompareExpr,
                        this.attachValueExprs);
                predicate.resolve(scope, fieldMappings, columnMetaToColRefMap);
                this.tblIdToAttachPredicateMap.put(tableId, predicate);
            }
        }
    }

    public ScanAttachPredicate getAttachInPredicate(long tableId) {
        return tblIdToAttachPredicateMap.get(tableId);
    }

    public void destroy() {
        this.attachValueExprs = null;
        this.slotRefMatchers = null;
        this.tblIdToAttachPredicateMap.clear();
    }

    public static boolean isAttachScanPredicateTable(long tableId) {
        ScanAttachPredicateContext context = getContext();
        if (context == null) {
            return false;
        } else {
            return context.tblIdToAttachPredicateMap.containsKey(tableId);
        }
    }

    public static ScanAttachPredicate getAttachScanPredicate(long tableId) {
        ScanAttachPredicateContext context = getContext();
        if (context == null) {
            return null;
        } else {
            return context.getAttachInPredicate(tableId);
        }
    }

    public static ScanAttachPredicateContext getContext() {
        return SCAN_ATTACH_PREDICATE_CONTEXT.get();
    }

    public static void beginAttachScanPredicate(
            SlotRef[] attachCompareExprs,
            LiteralExpr[] attachValueExprs) {
        Preconditions.checkNotNull(attachCompareExprs);
        Preconditions.checkNotNull(attachValueExprs);
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context == null) {
            context = new ScanAttachPredicateContext(OperatorType.IN, attachCompareExprs, attachValueExprs);
            SCAN_ATTACH_PREDICATE_CONTEXT.set(context);
        }
    }

    public static void prepareAttachScanPredicate(TableRelation tableRelation,
                                                  List<ColumnRefOperator> fieldMappings,
                                                  Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            TableName tableName = tableRelation.getName();
            long tableId = tableRelation.getTable().getId();
            Scope scope = tableRelation.getScope();
            context.prepare(tableId, tableName, scope, fieldMappings, columnMetaToColRefMap);
        }
    }

    public static void endAttachScanPredicate() {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context != null) {
            context.destroy();
            SCAN_ATTACH_PREDICATE_CONTEXT.set(null);
        }
    }
}
