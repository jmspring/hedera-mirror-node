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

import com.google.common.base.Stopwatch;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody.DataCase;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.codec.binary.Hex;
import org.springframework.scheduling.annotation.Scheduled;

import com.hedera.mirror.importer.domain.ApplicationStatusCode;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.exception.ParserException;
import com.hedera.mirror.importer.parser.FileParser;
import com.hedera.mirror.importer.parser.domain.RecordItem;
import com.hedera.mirror.importer.repository.ApplicationStatusRepository;
import com.hedera.mirror.importer.util.FileDelimiter;
import com.hedera.mirror.importer.util.ShutdownHelper;
import com.hedera.mirror.importer.util.Utility;

/**
 * This is a utility file to read back service record file generated by Hedera node
 */
@Log4j2
@Named
public class RecordFileParser implements FileParser {

    private final ApplicationStatusRepository applicationStatusRepository;
    private final RecordParserProperties parserProperties;
    private final MeterRegistry meterRegistry;
    private final RecordParsedItemHandler recordParsedItemHandler;
    private final RecordParserPostgresConnection connect;  // TODO: better way to do this?

    // Metrics
    private final Timer.Builder parseDurationMetric;
    private final Timer.Builder transactionLatencyMetric;
    private final DistributionSummary.Builder transactionSizeMetric;

    public RecordFileParser(ApplicationStatusRepository applicationStatusRepository,
                            RecordParserProperties parserProperties, RecordParsedItemHandler recordParsedItemHandler,
                            RecordParserPostgresConnection postgresConnection, MeterRegistry meterRegistry) {
        this.applicationStatusRepository = applicationStatusRepository;
        this.parserProperties = parserProperties;
        this.meterRegistry = meterRegistry;
        this.recordParsedItemHandler = recordParsedItemHandler;
        connect = postgresConnection;

        parseDurationMetric = Timer.builder("hedera.mirror.parse.duration")
                .description("The duration in ms it took to parse the file and store it in the database");

        transactionSizeMetric = DistributionSummary.builder("hedera.mirror.transaction.size")
                .description("The size of the transaction in bytes")
                .baseUnit("bytes");

        transactionLatencyMetric = Timer.builder("hedera.mirror.transaction.latency")
                .description("The difference in ms between the time consensus was achieved and the mirror node " +
                        "processed the transaction");
    }

    /**
     * @return 0 if row with given filename already exists, otherwise id of newly added row.
     */
    private long createFileRowIfNotExists(String filename) throws ParserException {
        try {
            long fileId;
            try (CallableStatement fileCreate = connect.getConnection().prepareCall("{? = call f_file_create( ? ) }")) {
                fileCreate.registerOutParameter(1, Types.BIGINT);
                fileCreate.setString(2, filename);
                fileCreate.execute();
                fileId = fileCreate.getLong(1);
            }
            if (fileId == 0) {
                log.trace("File {} already exists in the database.", filename);
                rollback();
            }
            log.trace("Added file {} to the database.", filename);
            return fileId;
        } catch (SQLException e) {
            throw new ParserException("Error saving file in database: " + filename, e);
        }
    }

    private void closeFileAndCommit(long fileId, String fileHash, String previousHash) throws ParserException {
        try (CallableStatement fileClose = connect.getConnection().prepareCall("{call f_file_complete( ?, ?, ? ) }")) {
            // execute any remaining batches
            recordParsedItemHandler.onFileComplete();

            // update the file to processed
            fileClose.setLong(1, fileId);

            if (Utility.hashIsEmpty(fileHash)) {
                fileClose.setObject(2, null);
            } else {
                fileClose.setString(2, fileHash);
            }

            if (Utility.hashIsEmpty(previousHash)) {
                fileClose.setObject(3, null);
            } else {
                fileClose.setString(3, previousHash);
            }

            fileClose.execute();
            // commit the changes to the database
            connect.getConnection().commit();
        } catch (SQLException e) {
            throw new ParserException("Error on commit ", e);
        }
    }

    private void rollback() {
        try {
            connect.getConnection().rollback();
        } catch (SQLException e) {
            log.error("Exception while rolling transaction back", e);
        }
    }

    /**
     * Given a service record name, read its prevFileHash
     *
     * @param fileName the name of record file to read
     * @return return previous file hash's Hex String
     */
    public static String readPrevFileHash(String fileName) {
        File file = new File(fileName);
        if (file.exists() == false) {
            log.warn("File does not exist {}", fileName);
            return null;
        }
        byte[] prevFileHash = new byte[48];
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            // record_format_version
            dis.readInt();
            // version
            dis.readInt();

            byte typeDelimiter = dis.readByte();

            if (typeDelimiter == FileDelimiter.RECORD_TYPE_PREV_HASH) {
                dis.read(prevFileHash);
                String hexString = Hex.encodeHexString(prevFileHash);
                log.trace("Read previous file hash {} for file {}", hexString, fileName);
                return hexString;
            } else {
                log.error("Expecting previous file hash, but found file delimiter {} for file {}", typeDelimiter,
                        fileName);
            }
        } catch (Exception e) {
            log.error("Error reading previous file hash {}", fileName, e);
        }

        return null;
    }

    /**
     * Given a service record name, read and parse and return as a list of service record pair
     *
     * @param fileName             the name of record file to read
     * @param expectedPrevFileHash the hash of the previous record file in the series
     * @param thisFileHash         the hash of this file
     * @return return boolean indicating method success
     * @throws Exception
     */
    public boolean loadRecordFile(String fileName, String expectedPrevFileHash, String thisFileHash) throws Exception {
        File file = new File(fileName);

        if (file.exists() == false) {
            log.warn("File does not exist {}", fileName);
            return false;
        }
        long counter = 0;
        byte[] readFileHash = new byte[48];
        long fileId;

        try {
            fileId = createFileRowIfNotExists(fileName);
            if (fileId == 0) {
                return true; // skip this file
            }
        } catch (ImporterException e) {
            log.error(e);
            rollback();
            return false;
        }
        Stopwatch stopwatch = Stopwatch.createStarted();
        Integer recordFileVersion = 0;
        Boolean success = false;

        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            recordFileVersion = dis.readInt();
            int version = dis.readInt();

            log.info("Loading version {} record file: {}", recordFileVersion, file.getName());

            while (dis.available() != 0) {

                try {
                    byte typeDelimiter = dis.readByte();

                    switch (typeDelimiter) {
                        case FileDelimiter.RECORD_TYPE_PREV_HASH:
                            dis.read(readFileHash);

                            if (Utility.hashIsEmpty(expectedPrevFileHash)) {
                                log.error("Previous file hash not available");
                                expectedPrevFileHash = Hex.encodeHexString(readFileHash);
                            }

                            String actualPrevFileHash = Hex.encodeHexString(readFileHash);

                            log.trace("New file hash = {}, old hash = {}", actualPrevFileHash, expectedPrevFileHash);

                            if (!actualPrevFileHash.contentEquals(expectedPrevFileHash)) {

                                if (applicationStatusRepository
                                        .findByStatusCode(ApplicationStatusCode.RECORD_HASH_MISMATCH_BYPASS_UNTIL_AFTER)
                                        .compareTo(Utility.getFileName(fileName)) < 0) {
                                    // last file for which mismatch is allowed is in the past
                                    log.error("Prev file hash mismatch for file {}. expected = {}, actual = {}",
                                            fileName,
                                            expectedPrevFileHash, actualPrevFileHash);
                                    rollback();
                                    return false;
                                }
                            }
                            break;
                        case FileDelimiter.RECORD_TYPE_RECORD:
                            counter++;

                            int byteLength = dis.readInt();
                            byte[] txRawBytes = new byte[byteLength];
                            dis.readFully(txRawBytes);
                            Transaction transaction = Transaction.parseFrom(txRawBytes);

                            byteLength = dis.readInt();
                            byte[] recordRawBytes = new byte[byteLength];
                            dis.readFully(recordRawBytes);
                            TransactionRecord txRecord = TransactionRecord.parseFrom(recordRawBytes);

                            try {
                                if (log.isTraceEnabled()) {
                                    log.trace("Transaction = {}, Record = {}", Utility
                                            .printProtoMessage(transaction), Utility.printProtoMessage(txRecord));
                                } else {
                                    log.debug("Storing transaction with consensus timestamp {}", () -> Utility
                                            .printProtoMessage(txRecord.getConsensusTimestamp()));
                                }
                                RecordFileLogger.storeRecord(new RecordItem(
                                        transaction, txRecord, txRawBytes, recordRawBytes));
                            } finally {
                                // TODO: Refactor to not parse TransactionBody twice
                                DataCase dc = Utility.getTransactionBody(transaction).getDataCase();
                                String type = dc != null && dc != DataCase.DATA_NOT_SET ? dc.name() : "UNKNOWN";
                                transactionSizeMetric.tag("type", type)
                                        .register(meterRegistry)
                                        .record(txRawBytes.length);

                                Instant consensusTimestamp = Utility
                                        .convertToInstant(txRecord.getConsensusTimestamp());
                                transactionLatencyMetric.tag("type", type)
                                        .register(meterRegistry)
                                        .record(Duration.between(consensusTimestamp, Instant.now()));
                            }
                            break;
                        case FileDelimiter.RECORD_TYPE_SIGNATURE:
                            int sigLength = dis.readInt();
                            byte[] sigBytes = new byte[sigLength];
                            dis.readFully(sigBytes);
                            log.trace("File {} has signature {}", fileName, Hex.encodeHexString(sigBytes));
                            break;

                        default:
                            log.error("Unknown record file delimiter {} for file {}", typeDelimiter, file);
                            rollback();
                            return false;
                    }
                } catch (Exception e) {
                    log.error("Exception {}", e);
                    rollback();
                    return false;
                }
            }

            log.trace("Calculated file hash for the current file {}", thisFileHash);
            closeFileAndCommit(fileId, thisFileHash, expectedPrevFileHash);

            if (!Utility.hashIsEmpty(thisFileHash)) {
                applicationStatusRepository
                        .updateStatusValue(ApplicationStatusCode.LAST_PROCESSED_RECORD_HASH, thisFileHash);
            }

            success = true;
        } catch (Exception e) {
            log.error("Error parsing record file {} after {}", file, stopwatch, e);
            rollback();
        } finally {
            log.info("Finished parsing {} transactions from record file {} in {}", counter, file
                    .getName(), stopwatch);

            parseDurationMetric.tag("type", "record")
                    .tag("success", success.toString())
                    .tag("version", recordFileVersion.toString())
                    .register(meterRegistry)
                    .record(stopwatch.elapsed());
        }

        return success;
    }

    /**
     * read and parse a list of record files
     *
     * @throws Exception
     */
    private void loadRecordFiles(List<String> fileNames) throws Exception {
        String prevFileHash = applicationStatusRepository
                .findByStatusCode(ApplicationStatusCode.LAST_PROCESSED_RECORD_HASH);
        Collections.sort(fileNames);

        for (String name : fileNames) {
            String thisFileHash = "";
            if (ShutdownHelper.isStopping()) {
                return;
            }
            thisFileHash = Hex.encodeHexString(Utility.getFileHash(name));
            if (loadRecordFile(name, prevFileHash, thisFileHash)) {
                prevFileHash = thisFileHash;
                Utility.moveFileToParsedDir(name, "/parsedRecordFiles/");
            } else {
                return;
            }
        }
    }

    @Override
    @Scheduled(fixedRateString = "${hedera.mirror.parser.record.frequency:500}")
    public void parse() {
        try {
            if (!parserProperties.isEnabled()) {
                return;
            }

            if (ShutdownHelper.isStopping()) {
                return;
            }

            Path path = parserProperties.getValidPath();
            log.debug("Parsing record files from {}", path);
            File file = path.toFile();
            if (file.isDirectory()) { //if it's a directory
                String[] files = file.list(); // get all files under the directory
                Arrays.sort(files);           // sorted by name (timestamp)

                // add directory prefix to get full path
                List<String> fullPaths = Arrays.asList(files).stream()
                        .filter(f -> Utility.isRecordFile(f))
                        .map(s -> file + "/" + s)
                        .collect(Collectors.toList());

                if (fullPaths != null && fullPaths.size() != 0) {
                    log.trace("Processing record files: {}", fullPaths);
                    loadRecordFiles(fullPaths);
                } else {
                    log.debug("No files to parse");
                }
            } else {
                log.error("Input parameter is not a folder: {}", path);
            }
        } catch (Exception e) {
            log.error("Error parsing files", e);
        }
    }
}
