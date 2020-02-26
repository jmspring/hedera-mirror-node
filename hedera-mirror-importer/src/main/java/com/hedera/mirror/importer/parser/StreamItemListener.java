package com.hedera.mirror.importer.parser;

import com.hedera.mirror.importer.exception.ImporterException;

public interface StreamItemListener<T> {
    void onItem(T item) throws ImporterException;
}
