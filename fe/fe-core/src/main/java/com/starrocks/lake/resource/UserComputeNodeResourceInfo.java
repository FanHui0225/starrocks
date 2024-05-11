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

package com.starrocks.lake.resource;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * Created by liujing on 2024/5/10.
 */
public class UserComputeNodeResourceInfo implements Writable {

    @SerializedName(value = "resourceUser")
    private String resourceUser;
    @SerializedName(value = "computeNodeIds")
    private Set<Long> computeNodeIds;

    public UserComputeNodeResourceInfo() {
    }

    public UserComputeNodeResourceInfo(String resourceUser, Set<Long> computeNodeIds) {
        this.resourceUser = resourceUser;
        this.computeNodeIds = computeNodeIds;
    }

    public Set<Long> getComputeNodeIds() {
        return computeNodeIds;
    }

    public String getResourceUser() {
        return resourceUser;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static UserComputeNodeResourceInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, UserComputeNodeResourceInfo.class);
    }

    @Override
    public String toString() {
        return "UserComputeNodeResourceInfo{" +
                "resourceUser='" + resourceUser + '\'' +
                ", computeNodeIds=" + computeNodeIds +
                '}';
    }
}
