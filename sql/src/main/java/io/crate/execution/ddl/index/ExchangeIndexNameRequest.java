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

public class ExchangeIndexNameRequest extends AcknowledgedRequest<ExchangeIndexNameRequest> {

    private String sourceIndexName;
    private String targetIndexName;

    ExchangeIndexNameRequest() {
    }

    public ExchangeIndexNameRequest(String sourceIndexName, String targetIndexName) {
        this.sourceIndexName = sourceIndexName;
        this.targetIndexName = targetIndexName;

    }

    public String sourceIndexName() {
        return sourceIndexName;
    }

    public String targetIndexName() {
        return targetIndexName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || targetIndexName == null || sourceIndexName.isEmpty() || targetIndexName.isEmpty()) {
            validationException = addValidationError("source and target index names must not be null/empty", null);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        sourceIndexName = in.readString();
        targetIndexName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
        out.writeString(targetIndexName);
    }
}
