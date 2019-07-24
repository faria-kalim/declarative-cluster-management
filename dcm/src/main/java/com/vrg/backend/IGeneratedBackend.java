/*
 * Copyright © 2018-2019 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vrg.backend;

import com.vrg.IRContext;
import com.vrg.IRTable;
import org.jooq.Record;
import org.jooq.Result;

import java.util.Map;

public interface IGeneratedBackend {
    Map<IRTable, Result<? extends Record>> solve(final IRContext context);
}
