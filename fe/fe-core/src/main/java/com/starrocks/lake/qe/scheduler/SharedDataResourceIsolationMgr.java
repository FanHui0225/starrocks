package com.starrocks.lake.qe.scheduler;

/**
 * Created by liujing on 2024/5/9.
 */
public class SharedDataResourceIsolationMgr implements ResourceIsolationMgr {

    private static final SharedDataResourceIsolationMgr SHARED_DATA_RESOURCE_ISOLATION_MGR = new SharedDataResourceIsolationMgr();

    public static SharedDataResourceIsolationMgr getManager() {
        return SHARED_DATA_RESOURCE_ISOLATION_MGR;
    }

    private SharedDataResourceIsolationMgr() {
    }
}
