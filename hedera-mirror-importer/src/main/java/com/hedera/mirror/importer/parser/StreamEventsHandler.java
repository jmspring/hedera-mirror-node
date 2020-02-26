package com.hedera.mirror.importer.parser;

import com.hedera.mirror.importer.exception.ImporterException;

/**
 * Invocation pattern: onBatchStart ...<events from sub-interfaces>... [onBatchComplete | onError]
 */
public interface StreamEventsHandler {
    /**
     * Called when starting processing of a batch of transactions.
     *
     * @param batchName depends on batch type; if streams are files, then filename.
     * @throws ImporterException
     */
    void onBatchStart(String batchName) throws ImporterException;

    /**
     * Called after successful handling of batch of transactions.
     *
     * @param batchName depends on batch type; if streams are files, then filename.
     * @throws ImporterException
     */
    void onBatchComplete(String batchName) throws ImporterException;

    /**
     * Called if an exception is thrown during processing of batch.
     */
    void onError(Throwable e);
}
