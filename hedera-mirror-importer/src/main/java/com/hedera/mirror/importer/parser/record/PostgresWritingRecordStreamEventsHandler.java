package com.hedera.mirror.importer.parser.record;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import javax.inject.Named;
import lombok.extern.log4j.Log4j2;

import com.hedera.mirror.importer.domain.ContractResult;
import com.hedera.mirror.importer.domain.CryptoTransfer;
import com.hedera.mirror.importer.domain.FileData;
import com.hedera.mirror.importer.domain.LiveHash;
import com.hedera.mirror.importer.domain.NonFeeTransfer;
import com.hedera.mirror.importer.domain.TopicMessage;
import com.hedera.mirror.importer.domain.Transaction;
import com.hedera.mirror.importer.exception.ImporterException;
import com.hedera.mirror.importer.exception.ParserException;
import com.hedera.mirror.importer.parser.domain.StreamFileInfo;
import com.hedera.mirror.importer.util.DatabaseUtilities;
import com.hedera.mirror.importer.util.Utility;

@Log4j2
@Named
public class PostgresWritingRecordStreamEventsHandler implements RecordStreamEventsHandler {
    public Connection connect = null;
    private PreparedStatement sqlInsertTransaction;
    private PreparedStatement sqlInsertTransferList;
    private PreparedStatement sqlInsertNonFeeTransfers;
    private PreparedStatement sqlInsertFileData;
    private PreparedStatement sqlInsertContractCall;
    private PreparedStatement sqlInsertClaimData;
    private PreparedStatement sqlInsertTopicMessage;
    private PostgresRecordWriterProperties properties;
    private long fileId = 0;
    private long batch_count = 0;

    PostgresWritingRecordStreamEventsHandler(PostgresRecordWriterProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean onBatchStart(StreamFileInfo fileInfo) throws ParserException {
        initConnection();
        initSqlStatements();
        if (!createFileRowIfNotExists(fileInfo.getFilename())) {
            closeStatementsAndConnection();
            return false; // skip file if row already exists
        }
        return true;
    }

    private void initConnection() throws ParserException {
        connect = DatabaseUtilities.openDatabase(connect);
        if (connect == null) {
            throw new ParserException("Unable to connect to database");
        }
        try {
            connect.setAutoCommit(false);  // do not auto-commit
        } catch (SQLException e) {
            throw new ParserException("Unable to set connection to not auto commit", e);
        }
    }

    /**
     * @return true if new row is added; false if a row with given filename already existed.
     */
    private boolean createFileRowIfNotExists(String filename) throws ParserException {
        try {
            fileId = 0;
            try (CallableStatement fileCreate = connect.prepareCall("{? = call f_file_create( ? ) }")) {
                fileCreate.registerOutParameter(1, Types.BIGINT);
                fileCreate.setString(2, filename);
                fileCreate.execute();
                fileId = fileCreate.getLong(1);
            }
            if (fileId == 0) {
                log.trace("File {} already exists in the database.", filename);
                return false;
            }
            log.trace("Added file {} to the database.", filename);
            return true;
        } catch (SQLException e) {
            throw new ParserException("Error saving file in database: " + filename, e);
        }
    }

    private void initSqlStatements() throws ParserException {
        try {
            sqlInsertTransaction = connect.prepareStatement("INSERT INTO t_transactions"
                    + " (fk_node_acc_id, memo, valid_start_ns, type, fk_payer_acc_id"
                    + ", result, consensus_ns, fk_cud_entity_id, charged_tx_fee"
                    + ", initial_balance, fk_rec_file_id, valid_duration_seconds, max_fee"
                    + ", transaction_hash, transaction_bytes)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            sqlInsertTransferList = connect.prepareStatement("INSERT INTO t_cryptotransferlists"
                    + " (consensus_timestamp, amount, realm_num, entity_num)"
                    + " VALUES (?, ?, ?, ?)");

            sqlInsertNonFeeTransfers = connect.prepareStatement("insert into non_fee_transfers"
                    + " (consensus_timestamp, amount, realm_num, entity_num)"
                    + " values (?, ?, ?, ?)");

            sqlInsertFileData = connect.prepareStatement("INSERT INTO t_file_data"
                    + " (consensus_timestamp, file_data)"
                    + " VALUES (?, ?)");

            sqlInsertContractCall = connect.prepareStatement("INSERT INTO t_contract_result"
                    + " (consensus_timestamp, function_params, gas_supplied, call_result, gas_used)"
                    + " VALUES (?, ?, ?, ?, ?)");

            sqlInsertClaimData = connect.prepareStatement("INSERT INTO t_livehashes"
                    + " (consensus_timestamp, livehash)"
                    + " VALUES (?, ?)");

            sqlInsertTopicMessage = connect.prepareStatement("insert into topic_message"
                    + " (consensus_timestamp, realm_num, topic_num, message, running_hash, sequence_number)"
                    + " values (?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            throw new ParserException("Unable to prepare SQL statements", e);
        }
    }

    @Override
    public void onBatchComplete(StreamFileInfo fileInfo) throws ImporterException {
        closeFileAndCommit(fileInfo.getFileHash(), fileInfo.getPrevFileHash());
        closeStatementsAndConnection();
    }

    private void closeFileAndCommit(String fileHash, String previousHash) throws ParserException {
        try (CallableStatement fileClose = connect.prepareCall("{call f_file_complete( ?, ?, ? ) }")) {
            // execute any remaining batches
            executeBatches();

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
            connect.commit();
        } catch (SQLException e) {
            throw new ParserException("Error on commit ", e);
        }
    }

    private void closeStatementsAndConnection() throws ParserException {
        try {
            sqlInsertFileData.close();
            sqlInsertTransferList.close();
            sqlInsertNonFeeTransfers.close();
            sqlInsertTransaction.close();
            sqlInsertContractCall.close();
            sqlInsertClaimData.close();
            sqlInsertTopicMessage.close();

            connect = DatabaseUtilities.closeDatabase(connect);
        } catch (SQLException e) {
            throw new ParserException("Error closing connection", e);
        }
    }

    private void executeBatches() throws SQLException {
        int[] transactions = sqlInsertTransaction.executeBatch();
        int[] transferLists = sqlInsertTransferList.executeBatch();
        int[] nonFeeTransfers = sqlInsertNonFeeTransfers.executeBatch();
        int[] fileData = sqlInsertFileData.executeBatch();
        int[] contractCalls = sqlInsertContractCall.executeBatch();
        int[] claimData = sqlInsertClaimData.executeBatch();
        int[] topicMessages = sqlInsertTopicMessage.executeBatch();
        log.info("Inserted {} transactions, {} transfer lists, {} files, {} contracts, {} claims, {} topic messages, " +
                        "{} non-fee transfers",
                transactions.length, transferLists.length, fileData.length, contractCalls.length, claimData.length,
                topicMessages.length, nonFeeTransfers.length);
        batch_count = 0;
    }

    @Override
    public void onError() {
        try {
            connect.rollback();
            closeStatementsAndConnection();
        } catch (SQLException e) {
            log.error("Exception while rolling transaction back", e);
        }
    }

    @Override
    public void onTransaction(Transaction transaction) throws ImporterException {
        try {
            // Temporary until we convert SQL statements to repository invocations
            if (transaction.getEntity() != null) {
                sqlInsertTransaction.setLong(F_TRANSACTION.CUD_ENTITY_ID.ordinal(), transaction.getEntity().getId());
            } else {
                sqlInsertTransaction.setObject(F_TRANSACTION.CUD_ENTITY_ID.ordinal(), null);
            }
            sqlInsertTransaction.setLong(F_TRANSACTION.FK_REC_FILE_ID.ordinal(), fileId);
            sqlInsertTransaction.setLong(F_TRANSACTION.FK_NODE_ACCOUNT_ID.ordinal(), transaction.getNodeAccountId());
            sqlInsertTransaction.setBytes(F_TRANSACTION.MEMO.ordinal(), transaction.getMemo());
            sqlInsertTransaction.setLong(F_TRANSACTION.VALID_START_NS.ordinal(), transaction.getValidStartNs());
            sqlInsertTransaction.setInt(F_TRANSACTION.TYPE.ordinal(), transaction.getType());
            sqlInsertTransaction.setLong(F_TRANSACTION.VALID_DURATION_SECONDS.ordinal(),
                    transaction.getValidDurationSeconds());
            sqlInsertTransaction.setLong(F_TRANSACTION.FK_PAYER_ACCOUNT_ID.ordinal(), transaction.getPayerAccountId());
            sqlInsertTransaction.setLong(F_TRANSACTION.RESULT.ordinal(), transaction.getResult());
            sqlInsertTransaction.setLong(F_TRANSACTION.CONSENSUS_NS.ordinal(), transaction.getConsensusNs());
            sqlInsertTransaction.setLong(F_TRANSACTION.CHARGED_TX_FEE.ordinal(), transaction.getChargedTxFee());
            sqlInsertTransaction.setLong(F_TRANSACTION.MAX_FEE.ordinal(), transaction.getMaxFee());
            sqlInsertTransaction.setBytes(F_TRANSACTION.TRANSACTION_HASH.ordinal(), transaction.getTransactionHash());
            sqlInsertTransaction.setBytes(F_TRANSACTION.TRANSACTION_BYTES.ordinal(), transaction.getTransactionBytes());
            sqlInsertTransaction.setLong(F_TRANSACTION.INITIAL_BALANCE.ordinal(), transaction.getInitialBalance());
            sqlInsertTransaction.addBatch();

            if (batch_count == properties.getBatchSize() - 1) {
                // execute any remaining batches
                executeBatches();
            } else {
                batch_count += 1;
            }
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onCryptoTransferList(CryptoTransfer cryptoTransfer) throws ImporterException {
        try {
            sqlInsertTransferList.setLong(F_TRANSFERLIST.CONSENSUS_TIMESTAMP.ordinal(),
                    cryptoTransfer.getConsensusTimestamp());
            sqlInsertTransferList.setLong(F_TRANSFERLIST.REALM_NUM.ordinal(), cryptoTransfer.getRealmNum());
            sqlInsertTransferList.setLong(F_TRANSFERLIST.ENTITY_NUM.ordinal(), cryptoTransfer.getEntityNum());
            sqlInsertTransferList.setLong(F_TRANSFERLIST.AMOUNT.ordinal(), cryptoTransfer.getAmount());
            sqlInsertTransferList.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onNonFeeTransfer(NonFeeTransfer nonFeeTransfer) throws ImporterException {
        try {
            sqlInsertNonFeeTransfers.setLong(F_NONFEETRANSFER.CONSENSUS_TIMESTAMP.ordinal(),
                    nonFeeTransfer.getConsensusTimestamp());
            sqlInsertNonFeeTransfers.setLong(F_NONFEETRANSFER.AMOUNT.ordinal(), nonFeeTransfer.getAmount());
            sqlInsertNonFeeTransfers.setLong(F_NONFEETRANSFER.REALM_NUM.ordinal(), nonFeeTransfer.getRealmNum());
            sqlInsertNonFeeTransfers.setLong(F_NONFEETRANSFER.ENTITY_NUM.ordinal(), nonFeeTransfer.getEntityNum());
            sqlInsertNonFeeTransfers.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onTopicMessage(TopicMessage topicMessage) throws ImporterException {
        try {
            sqlInsertTopicMessage.setLong(F_TOPICMESSAGE.CONSENSUS_TIMESTAMP.ordinal(),
                    topicMessage.getConsensusTimestamp());
            sqlInsertTopicMessage.setShort(F_TOPICMESSAGE.REALM_NUM.ordinal(), (short) topicMessage.getRealmNum());
            sqlInsertTopicMessage.setInt(F_TOPICMESSAGE.TOPIC_NUM.ordinal(), topicMessage.getTopicNum());
            sqlInsertTopicMessage.setBytes(F_TOPICMESSAGE.MESSAGE.ordinal(), topicMessage.getMessage());
            sqlInsertTopicMessage.setBytes(F_TOPICMESSAGE.RUNNING_HASH.ordinal(), topicMessage.getRunningHash());
            sqlInsertTopicMessage.setLong(F_TOPICMESSAGE.SEQUENCE_NUMBER.ordinal(), topicMessage.getSequenceNumber());
            sqlInsertTopicMessage.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onContractResult(ContractResult contractResult) throws ImporterException {
        try {
            sqlInsertContractCall.setLong(F_CONTRACT_CALL.CONSENSUS_TIMESTAMP.ordinal(),
                    contractResult.getConsensusTimestamp());
            sqlInsertContractCall
                    .setBytes(F_CONTRACT_CALL.FUNCTION_PARAMS.ordinal(), contractResult.getFunctionParameters());
            sqlInsertContractCall.setLong(F_CONTRACT_CALL.GAS_SUPPLIED.ordinal(), contractResult.getGasSupplied());
            sqlInsertContractCall.setBytes(F_CONTRACT_CALL.CALL_RESULT.ordinal(), contractResult.getCallResult());
            sqlInsertContractCall.setLong(F_CONTRACT_CALL.GAS_USED.ordinal(), contractResult.getGasUsed());
            sqlInsertContractCall.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onFileData(FileData fileData) throws ImporterException {
        try {
            sqlInsertFileData.setLong(F_FILE_DATA.CONSENSUS_TIMESTAMP.ordinal(), fileData.getConsensusTimestamp());
            sqlInsertFileData.setBytes(F_FILE_DATA.FILE_DATA.ordinal(), fileData.getFileData());
            sqlInsertFileData.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    @Override
    public void onLiveHash(LiveHash liveHash) throws ImporterException {
        try {
            sqlInsertClaimData.setLong(F_LIVEHASH_DATA.CONSENSUS_TIMESTAMP.ordinal(), liveHash.getConsensusTimestamp());
            sqlInsertClaimData.setBytes(F_LIVEHASH_DATA.LIVEHASH.ordinal(), liveHash.getLivehash());
            sqlInsertClaimData.addBatch();
        } catch (SQLException e) {
            throw new ParserException(e);
        }
    }

    enum F_TRANSACTION {
        ZERO // column indices start at 1, this creates the necessary offset
        , FK_NODE_ACCOUNT_ID, MEMO, VALID_START_NS, TYPE, FK_PAYER_ACCOUNT_ID, RESULT, CONSENSUS_NS,
        CUD_ENTITY_ID, CHARGED_TX_FEE, INITIAL_BALANCE, FK_REC_FILE_ID, VALID_DURATION_SECONDS, MAX_FEE,
        TRANSACTION_HASH, TRANSACTION_BYTES
    }

    enum F_TOPICMESSAGE {
        ZERO // column indices start at 1, this creates the necessary offset
        , CONSENSUS_TIMESTAMP, REALM_NUM, TOPIC_NUM, MESSAGE, RUNNING_HASH, SEQUENCE_NUMBER
    }

    enum F_TRANSFERLIST {
        ZERO // column indices start at 1, this creates the necessary offset
        , CONSENSUS_TIMESTAMP, AMOUNT, REALM_NUM, ENTITY_NUM
    }

    enum F_NONFEETRANSFER {
        ZERO // column indices start at 1, this creates the necessary offset
        , CONSENSUS_TIMESTAMP, AMOUNT, REALM_NUM, ENTITY_NUM
    }

    enum F_FILE_DATA {
        ZERO, CONSENSUS_TIMESTAMP, FILE_DATA
    }

    enum F_CONTRACT_CALL {
        ZERO, CONSENSUS_TIMESTAMP, FUNCTION_PARAMS, GAS_SUPPLIED, CALL_RESULT, GAS_USED
    }

    enum F_LIVEHASH_DATA {
        ZERO, CONSENSUS_TIMESTAMP, LIVEHASH
    }
}
