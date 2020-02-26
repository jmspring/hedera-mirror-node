package com.hedera.mirror.importer.parser.domain;

import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import lombok.Value;

@Value
public class RecordItem {
    private final Transaction transaction;
    private final TransactionRecord record;
    private final byte[] transactionRawBytes;
}
