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

package io.crate.execution.engine.distribution.merge;

import io.crate.data.BatchIterator;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.function.BooleanSupplier;

public final class NewBatchPagingIterator<Key, Item> implements BatchIterator<Item> {

    private final PagingIterator<Key, Item> pagingIterator;
    private Iterator<Item> it;
    private final BooleanSupplier areUpstreamsExhausted;
    private Item current;

    public NewBatchPagingIterator(PagingIterator<Key, Item> pagingIterator,
                                  BooleanSupplier areUpstreamsExhausted) {
        this.pagingIterator = pagingIterator;
        this.it = pagingIterator;
        this.areUpstreamsExhausted = areUpstreamsExhausted;
    }

    @Override
    public Item currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        it = pagingIterator.repeat().iterator();
        current = null;
    }

    @Override
    public boolean moveNext() {
        if (it.hasNext()) {
            current = it.next();
            return true;
        } else {
            current = null;
            return false;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public CompletionStage<?> loadNextBatch() {
        return null;
    }

    @Override
    public boolean allLoaded() {
        return areUpstreamsExhausted.getAsBoolean();
    }

    @Override
    public void kill(@Nonnull Throwable throwable) {
    }
}
