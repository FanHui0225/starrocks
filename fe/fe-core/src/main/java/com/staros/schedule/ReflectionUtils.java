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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/task/AgentBatchTask.java

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

package com.staros.schedule;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Created by liujing on 2024/5/15.
 */
public class ReflectionUtils {

    public final static <R> R getFieldValue(String fieldName, Object v) throws IllegalAccessException {
        return getFieldValue(getDeclaredField(v, fieldName), v);
    }

    public final static <R> R getFieldValue(Field field, Object object) throws IllegalAccessException {
        if (field != null) {
            field.setAccessible(true);
            return (R) field.get(object);
        } else {
            return null;
        }
    }

    public final static void setFieldValue(Object object, String fieldName, Object value) throws IllegalAccessException {
        setFieldValue(object, getDeclaredField(object, fieldName), value);
    }

    public final static void setFieldValue(Object object, Field field, Object value) throws IllegalAccessException {
        if (field != null) {
            field.setAccessible(true);
            field.set(object, value);
        }
    }

    public final static Field getDeclaredField(Object v, String fieldName) {
        return getDeclaredField(v.getClass(), fieldName);
    }

    public final static Field getDeclaredField(Class<?> cls, String fieldName) {
        return getDeclaredField(cls, fieldName, false);
    }

    public final static Field getDeclaredStaticField(Class<?> cls, String fieldName) {
        return getDeclaredField(cls, fieldName, true);
    }

    protected final static Field getDeclaredField(Class<?> cls, String fieldName, boolean isStatic) {
        Field field;
        Class<?> clazz = cls;
        for (; clazz != Object.class; clazz = clazz.getSuperclass()) {
            try {
                field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                if (isStatic) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        return field;
                    } else
                        continue;
                } else {
                    return field;
                }
            } catch (Exception e) {
            }
        }
        return null;
    }
}
