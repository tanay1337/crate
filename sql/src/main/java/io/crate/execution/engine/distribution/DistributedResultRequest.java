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

package io.crate.execution.engine.distribution;

import io.crate.data.Bucket;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DistributedResultRequest extends TransportRequest {

    private byte inputId;
    private int executionPhaseId;
    private int bucketIdx;

    private List<DataType> columnTypes;
    private Bucket rows;
    private UUID jobId;
    private boolean isLast = true;

    private Throwable throwable = null;
    private boolean isKilled = false;

    public DistributedResultRequest() {
    }

    private DistributedResultRequest(UUID jobId, byte inputId, int executionPhaseId, int bucketIdx) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.bucketIdx = bucketIdx;
        this.inputId = inputId;
    }

    public DistributedResultRequest(UUID jobId,
                                    int executionPhaseId,
                                    byte inputId,
                                    int bucketIdx,
                                    List<DataType> columnTypes,
                                    Bucket rows,
                                    boolean isLast) {
        this(jobId, inputId, executionPhaseId, bucketIdx);
        this.columnTypes = columnTypes;
        this.rows = rows;
        this.isLast = isLast;
    }

    public DistributedResultRequest(UUID jobId,
                                    int executionPhaseId,
                                    byte inputId,
                                    int bucketIdx,
                                    Throwable throwable,
                                    boolean isKilled) {
        this(jobId, inputId, executionPhaseId, bucketIdx);
        this.throwable = throwable;
        this.isKilled = isKilled;
    }

    public UUID jobId() {
        return jobId;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    public byte executionPhaseInputId() {
        return inputId;
    }

    public int bucketIdx() {
        return bucketIdx;
    }

    public Bucket rows() {
        return rows;
    }

    public boolean isLast() {
        return isLast;
    }

    @Nullable
    public Throwable throwable() {
        return throwable;
    }

    public boolean isKilled() {
        return isKilled;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        executionPhaseId = in.readVInt();
        bucketIdx = in.readVInt();
        isLast = in.readBoolean();
        inputId = in.readByte();

        boolean failure = in.readBoolean();
        if (failure) {
            throwable = in.readException();
            isKilled = in.readBoolean();
        } else {
            int numColumns = in.readVInt();
            columnTypes = new ArrayList<>(numColumns);
            for (int i = 0; i < numColumns; i++) {
                columnTypes.add(DataTypes.fromStream(in));
            }
            int numRows = in.readVInt();
            ArrayList<Object[]> objects = new ArrayList<>(numRows);
            for (int r = 0; r < numRows; r++) {
                Object[] row = new Object[columnTypes.size()];
                for (int c = 0; c < columnTypes.size(); c++) {
                    row[c] = columnTypes.get(c).streamer().readValueFrom(in);
                }
                objects.add(row);
            }
            rows = new CollectionBucket(objects);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(executionPhaseId);
        out.writeVInt(bucketIdx);
        out.writeBoolean(isLast);
        out.writeByte(inputId);

        boolean failure = throwable != null;
        out.writeBoolean(failure);
        if (failure) {
            out.writeException(throwable);
            out.writeBoolean(isKilled);
        } else {
            out.writeVInt(columnTypes.size());
            for (DataType columnType: columnTypes) {
                DataTypes.toStream(columnType, out);
            }
            out.writeVInt(rows.size());
            for (Row row: rows) {
                assert row.numColumns() == columnTypes.size() : "columns and row size must match";
                for (int i = 0; i < row.numColumns(); i++) {
                    //noinspection unchecked
                    columnTypes.get(i).streamer().writeValueTo(out, row.get(i));
                }
            }
        }
    }
}
