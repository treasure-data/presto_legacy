/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.memory.context;

import com.google.common.util.concurrent.ListenableFuture;

import static java.util.Objects.requireNonNull;

class RootAggregatedMemoryContext
        extends AggregatedMemoryContext
{
    private final MemoryReservationHandler reservationHandler;
    private final long guaranteedMemory;

    RootAggregatedMemoryContext(MemoryReservationHandler reservationHandler, long guaranteedMemory)
    {
        this.reservationHandler = requireNonNull(reservationHandler, "reservationHandler is null");
        this.guaranteedMemory = guaranteedMemory;
    }

    synchronized ListenableFuture<?> updateBytes(long bytes)
    {
        ListenableFuture<?> future = reservationHandler.reserveMemory(bytes);
        addBytes(bytes);
        // make sure we never block queries below guaranteedMemory
        if (getBytes() < guaranteedMemory) {
            future = NOT_BLOCKED;
        }
        return future;
    }

    synchronized boolean tryUpdateBytes(long delta)
    {
        if (reservationHandler.tryReserveMemory(delta)) {
            addBytes(delta);
            return true;
        }
        return false;
    }

    synchronized AggregatedMemoryContext getParent()
    {
        return null;
    }

    void closeContext()
    {
        reservationHandler.reserveMemory(-getBytes());
    }
}
