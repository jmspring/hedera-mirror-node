package com.hedera.mirror.importer.parser.record;

/*-
 * ‌
 * Hedera Mirror Node
 * ​
 * Copyright (C) 2019 Hedera Hashgraph, LLC
 * ​
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ‍
 */

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

import com.hedera.mirror.importer.domain.RecordFile;

public class RecordFileLoggerTest extends AbstractRecordFileLoggerTest {

    @Test
    void initFile() {
        RecordFileLogger.completeFile(streamFileInfo);
        assertEquals(1, recordFileRepository.findByName(FILENAME).size());
    }

    @Test
    void initFileDuplicate() {
        RecordFileLogger.completeFile(streamFileInfo);
        assertEquals(RecordFileLogger.INIT_RESULT.SKIP, RecordFileLogger.initFile(streamFileInfo));
    }

    @Test
    void checkHashes() {
        RecordFileLogger.completeFile(streamFileInfo);
        List<RecordFile> recordFileList = recordFileRepository.findByName(FILENAME);
        assertEquals(1, recordFileList.size());
        RecordFile recordFile = recordFileList.get(0);
        assertEquals(FILE_HASH, recordFile.getFileHash());
        assertEquals(PREV_FILE_HASH, recordFile.getPreviousHash());
    }

    @Test
    void rollback() {
        RecordFileLogger.rollback();
        assertEquals(0, recordFileRepository.findByName(FILENAME).size());
    }
}
