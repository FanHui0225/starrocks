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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/SetExecutor.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authentication.PlainPasswordAuthenticationProvider;
import com.starrocks.authentication.UserAuthenticationInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SetListItem;
import com.starrocks.sql.ast.SetPassVar;
import com.starrocks.sql.ast.SetResourceIsolationVar;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.ast.UserVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// Set executor
public class SetExecutor {
    private static final Logger LOG = LogManager.getLogger(SetExecutor.class);

    private final ConnectContext ctx;
    private final SetStmt stmt;

    public SetExecutor(ConnectContext ctx, SetStmt stmt) {
        this.ctx = ctx;
        this.stmt = stmt;
    }

    private void setVariablesOfAllType(SetListItem var) throws DdlException {
        if (var instanceof SystemVariable) {
            ctx.modifySystemVariable((SystemVariable) var, false);
        } else if (var instanceof UserVariable) {
            UserVariable userVariable = (UserVariable) var;
            if (userVariable.getEvaluatedExpression() == null) {
                userVariable.deriveUserVariableExpressionResult(ctx);
            }

            ctx.modifyUserVariable(userVariable);
        } else if (var instanceof SetPassVar) {
            // Set password
            SetPassVar setPassVar = (SetPassVar) var;
            UserAuthenticationInfo userAuthenticationInfo = GlobalStateMgr.getCurrentState()
                    .getAuthenticationMgr()
                    .getUserAuthenticationInfoByUserIdentity(setPassVar.getUserIdent());
            if (null == userAuthenticationInfo) {
                throw new DdlException("authentication info for user " + setPassVar.getUserIdent() + " not found");
            }
            if (!userAuthenticationInfo.getAuthPlugin().equals(PlainPasswordAuthenticationProvider.PLUGIN_NAME)) {
                throw new DdlException("only allow set password for native user, current user: " +
                        setPassVar.getUserIdent() + ", AuthPlugin: " + userAuthenticationInfo.getAuthPlugin());
            }
            userAuthenticationInfo.setPassword(setPassVar.getPassword());
            GlobalStateMgr.getCurrentState().getAuthenticationMgr()
                    .alterUser(setPassVar.getUserIdent(), userAuthenticationInfo);
        } else if (var instanceof SetResourceIsolationVar) {
            SetResourceIsolationVar setResourceIsolationVar = (SetResourceIsolationVar) var;
            Map<UserIdentity, UserAuthenticationInfo> userToAuthenticationInfo =
                    GlobalStateMgr.getCurrentState().getAuthenticationMgr().getUserToAuthenticationInfo();
            UserIdentity userAuthenticationInfo = userToAuthenticationInfo.keySet().stream()
                    .filter(entry -> entry.getUser().equals(setResourceIsolationVar.getUserIdent().getUser()))
                    .findFirst()
                    .orElse(null);
            if (null == userAuthenticationInfo) {
                throw new DdlException("authentication info for user " +
                        setResourceIsolationVar.getUserIdent() + " not found");
            }
            UserIdentity currentUser = ctx.getCurrentUserIdentity();
            if (!currentUser.getUser().equals(AuthenticationMgr.ROOT_USER)) {
                throw new DdlException("only allow set resource isolation for other user," +
                        " current user: " + currentUser.getUser());
            }
            LOG.info("Debug =================>> Set Resource Isolation Var Proposal Successful!, " +
                            "user: {}, hosts: {}  <<=================",
                    setResourceIsolationVar.getUserIdent().getUser(),
                    setResourceIsolationVar.getHosts());
            GlobalStateMgr.getCurrentState()
                    .getComputeNodeResourceIsolationMgr()
                    .setUserComputeNodeResource(userAuthenticationInfo, setResourceIsolationVar.getHosts());
        }
    }

    /**
     * SetExecutor will set the session variables and password
     *
     * @throws DdlException
     */
    public void execute() throws DdlException {
        for (SetListItem var : stmt.getSetListItems()) {
            setVariablesOfAllType(var);
        }
    }
}
