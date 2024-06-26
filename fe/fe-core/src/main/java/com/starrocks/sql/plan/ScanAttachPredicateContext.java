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

import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;

/**
 * Created by liujing on 2024/6/25.
 */
public final class ScanAttachPredicateContext {

    private static final ThreadLocal<ScanAttachPredicateContext>
            SCAN_ATTACH_PREDICATE_CONTEXT = new ThreadLocal<>();

    private OperatorType opType;
    private SlotRef attachCompareExpr;
    private List<LiteralExpr> attachValueExprs;

    private ScanAttachPredicateContext(OperatorType opType) {
        this.opType = opType;
    }

    public OperatorType getOpType() {
        return opType;
    }

    public static void beginInPredicate(SlotRef attachCompareExpr, List<LiteralExpr> attachValueExprs) {
        ScanAttachPredicateContext context = SCAN_ATTACH_PREDICATE_CONTEXT.get();
        if (context == null) {
            context = new ScanAttachPredicateContext(OperatorType.IN);
            SCAN_ATTACH_PREDICATE_CONTEXT.set(context);
        }
        context.attachCompareExpr = attachCompareExpr;
        context.attachValueExprs = attachValueExprs;
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
