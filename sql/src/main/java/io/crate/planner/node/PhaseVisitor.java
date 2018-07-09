/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.node;

import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.HashJoinPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NestedLoopPhase;
import io.crate.execution.dsl.phases.PKLookupPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Get output {@link io.crate.Streamer}s for {@link ExecutionPhase}s
 */
public final class PhaseVisitor {

    private static final ExecutionPhaseStreamerVisitor EXECUTION_PHASE_STREAMER_VISITOR = new ExecutionPhaseStreamerVisitor();

    private PhaseVisitor() {
    }

    public static List<DataType> typesFromOutputs(ExecutionPhase executionPhase) {
        return EXECUTION_PHASE_STREAMER_VISITOR.process(executionPhase, null);
    }

    private static class ExecutionPhaseStreamerVisitor extends ExecutionPhaseVisitor<Void, List<DataType>> {

        private static final List<DataType> COUNT_TYPES = Collections.singletonList(DataTypes.LONG);

        @Override
        public List<DataType> visitMergePhase(MergePhase phase, Void context) {
            return phase.outputTypes();
        }

        @Override
        public List<DataType> visitRoutedCollectPhase(RoutedCollectPhase phase, Void context) {
            return phase.outputTypes();
        }

        @Override
        public List<DataType> visitPKLookup(PKLookupPhase pkLookupPhase, Void context) {
            return pkLookupPhase.outputTypes();
        }

        @Override
        public List<DataType> visitCountPhase(CountPhase phase, Void context) {
            return COUNT_TYPES;
        }

        @Override
        public List<DataType> visitNestedLoopPhase(NestedLoopPhase phase, Void context) {
            return phase.outputTypes();
        }

        @Override
        public List<DataType> visitHashJoinPhase(HashJoinPhase phase, Void context) {
            return phase.outputTypes();
        }

        @Override
        public List<DataType> visitFileUriCollectPhase(FileUriCollectPhase phase, Void context) {
            return phase.outputTypes();
        }

        @Override
        public List<DataType> visitTableFunctionCollect(TableFunctionCollectPhase phase, Void context) {
            return visitRoutedCollectPhase(phase, context);
        }

        @Override
        protected List<DataType> visitExecutionPhase(ExecutionPhase node, Void context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Got unsupported ExecutionNode %s", node.getClass().getName()));
        }
    }
}
