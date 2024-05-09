package com.starrocks.lake.qe.scheduler;

import com.starrocks.server.GlobalStateMgr;

/**
 * Created by liujing on 2024/5/9.
 */
public class SharedDataResourceIsolationMgr implements ResourceIsolationMgr {

    private static final SharedDataResourceIsolationMgr sharedDataResourceIsolationMgr = new SharedDataResourceIsolationMgr();

    public static SharedDataResourceIsolationMgr getManager() {
        return sharedDataResourceIsolationMgr;
    }

    private SharedDataResourceIsolationMgr() {
    }
}
