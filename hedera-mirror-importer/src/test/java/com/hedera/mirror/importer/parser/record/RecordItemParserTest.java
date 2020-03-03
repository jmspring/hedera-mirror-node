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
import java.util.Optional;
import org.junit.jupiter.api.Test;

import com.hedera.mirror.importer.domain.RecordFile;

public class RecordItemParserTest extends AbstractRecordItemParserTest {

    @Test
    void initFile() throws Exception {
        // when
        recordItemParser.completeFile("", ""); // corresponding initFile is in beforeEach()

        // expect
        assertEquals(1, recordFileRepository.findByName(FILE_NAME).size());
    }

    @Test
    void initFileDuplicate() throws Exception {
        // given: file already processed
        recordItemParser.completeFile("", ""); // corresponding initFile is in beforeEach()

        // when
        var result = recordItemParser.initFile(FILE_NAME);

        // then
        assertEquals(RecordItemParser.INIT_RESULT.SKIP, result);
    }

    private void checkFileAndHashes(String fileName, String fileHash, String prevFileHash) {
        List<RecordFile> recordFileList = recordFileRepository.findByName(fileName);
        assertEquals(1, recordFileList.size());
        RecordFile recordFile = recordFileList.get(0);
        assertEquals(fileHash, recordFile.getFileHash());
        assertEquals(prevFileHash, recordFile.getPreviousHash());
    }

    @Test
    void completeFileNoHashes() throws Exception {
        // when
        recordItemParser.completeFile("", ""); // corresponding initFile is in beforeEach()

        // then
        checkFileAndHashes(FILE_NAME, null, null);
    }

    @Test
    void completeFileWithHashes() throws Exception {
        // when
        recordItemParser.completeFile("123", "456"); // corresponding initFile is in beforeEach()

        // then
        checkFileAndHashes(FILE_NAME, "123", "456");
    }

    @Test
    void rollback() {
        // when
        recordItemParser.rollback();

        // then
        assertTrue(recordFileRepository.findByName(FILE_NAME).isEmpty());
    }
}
