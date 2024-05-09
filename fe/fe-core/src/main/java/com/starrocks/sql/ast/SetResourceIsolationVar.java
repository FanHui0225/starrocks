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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * Created by liujing on 2024/5/9.
 */
public class SetResourceIsolationVar extends SetListItem {

    private UserIdentity userIdent;

    private List<String> hosts;

    public SetResourceIsolationVar(UserIdentity userIdent, List<String> hosts) {
        this(userIdent, hosts, NodePosition.ZERO);
    }

    public SetResourceIsolationVar(UserIdentity userIdent, List<String> hosts, NodePosition pos) {
        super(pos);
        this.userIdent = userIdent;
        this.hosts = hosts;
    }

    public UserIdentity getUserIdent() {
        return userIdent;
    }

    public void setUserIdent(UserIdentity userIdent) {
        this.userIdent = userIdent;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }
}
