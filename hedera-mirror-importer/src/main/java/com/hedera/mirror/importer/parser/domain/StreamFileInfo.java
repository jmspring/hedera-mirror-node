package com.hedera.mirror.importer.parser.domain;

import lombok.Value;

@Value
public class StreamFileInfo {
    String filename;
    String fileHash;
    String prevFileHash;
}
