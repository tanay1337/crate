/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.iothub.processor;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import io.crate.iothub.operations.EventIngestService;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

public class EventProcessor implements IEventProcessor {
    private int checkpointBatchingCount = 0;

    private final String ingestionTable;
    private final EventIngestService ingestService;

    public EventProcessor(String ingestionTable, EventIngestService ingestService) {
        this.ingestionTable = ingestionTable;
        this.ingestService = ingestService;
    }

    private static final Logger LOGGER = Loggers.getLogger(EventProcessor.class);

    // OnOpen is called when a new event processor instance is created by the host.
    @Override
    public void onOpen(PartitionContext context) throws Exception {
        LOGGER.info("SAMPLE: Partition " + context.getPartitionId() + " is opening");
    }

    // OnClose is called when an event processor instance is being shut down.
    @Override
    public void onClose(PartitionContext context, CloseReason reason) throws Exception {
        LOGGER.info("SAMPLE: Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
    }

    // onError is called when an error occurs in EventProcessorHost code that is tied to this partition, such as a receiver failure.
    @Override
    public void onError(PartitionContext context, Throwable error) {
        LOGGER.error("SAMPLE: Partition " + context.getPartitionId() + " onError: " + error.toString());
    }

    // onEvents is called when events are received on this partition of the Event Hub.
    @Override
    public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {
        //LOGGER.info("SAMPLE: Partition " + context.getPartitionId() + " got event batch");
        int eventCount = 0;
        for (EventData data : events) {
            try {
                ingestService.doInsert(context, data, ingestionTable);
                eventCount++;

                // Checkpointing persists the current position in the event stream for this partition and means that the next
                // time any host opens an event processor on this event hub+consumer group+partition combination, it will start
                // receiving at the event after this one.
                this.checkpointBatchingCount++;
                if ((checkpointBatchingCount % 5) == 0) {
                   // LOGGER.info("SAMPLE: Partition " + context.getPartitionId() + " checkpointing at " +
                     //                  data.getSystemProperties().getOffset() + "," + data.getSystemProperties().getSequenceNumber());
                    // Checkpoints are created asynchronously. It is important to wait for the result of checkpointing
                    // before exiting onEvents or before creating the next checkpoint, to detect errors and to ensure proper ordering.
                    context.checkpoint(data).get();
                }
            } catch (Exception e) {
                LOGGER.error("Processing failed for an event: " + e.toString());
            }
        }
        //LOGGER.info("SAMPLE: Partition " + context.getPartitionId() + " batch size was " + eventCount + " for host " + context.getOwner());
    }
}
