package com.hedera.mirror.importer.parser;

import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.parser.domain.StreamFileInfo;

/**
 * Invocation pattern: onBatchStart [ ...<events from sub-interfaces>... [onBatchComplete | onError] ]
 */
public interface StreamEventsHandler {
    /**
     * Called when starting processing of a batch of transactions.
     *
     * @param fileInfo
     * @return true if batch processing should continue, false to skip the batch.
     * @throws ImporterException
     */
    boolean onBatchStart(StreamFileInfo fileInfo) throws ImporterException;

    /**
     * Called after successful handling of batch of transactions.
     *
     * @param fileInfo
     * @throws ImporterException
     */
    void onBatchComplete(StreamFileInfo fileInfo) throws ImporterException;

    /**
     * Called if an error is encountered during processing of batch.
     */
    void onError();
}
