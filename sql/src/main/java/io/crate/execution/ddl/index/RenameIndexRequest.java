/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl.index;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RenameIndexRequest extends AcknowledgedRequest<RenameIndexRequest> {

    private String[] sourceIndexName;
    private String[] targetIndexName;

    RenameIndexRequest() {
    }

    public RenameIndexRequest(String[] sourceIndexName, String[] targetIndexName) {
        this.sourceIndexName = sourceIndexName;
        this.targetIndexName = targetIndexName;

    }

    public String[] sourceIndexName() {
        return sourceIndexName;
    }

    public String[] targetIndexName() {
        return targetIndexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || targetIndexName == null || sourceIndexName.length == 0 || targetIndexName.length == 0) {
            validationException = addValidationError("source and target index names must not be null/empty", null);
        } else if (sourceIndexName.length != targetIndexName.length) {
            validationException = addValidationError("source and target input indexes must be of the same size", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceIndexName = in.readStringArray();
        targetIndexName = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(sourceIndexName);
        out.writeStringArray(targetIndexName);
    }
}
