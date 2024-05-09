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
