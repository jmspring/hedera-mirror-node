package com.hedera.mirror.importer.parser.record;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.annotation.PreDestroy;
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

@Log4j2
@Named
public class PostgresWritingRecordParsedItemHandler implements RecordParsedItemHandler {
    private long batch_count = 0;
    private PreparedStatement sqlInsertTransaction;
    private PreparedStatement sqlInsertTransferList;
    private PreparedStatement sqlInsertNonFeeTransfers;
    private PreparedStatement sqlInsertFileData;
    private PreparedStatement sqlInsertContractCall;
    private PreparedStatement sqlInsertClaimData;
    private PreparedStatement sqlInsertTopicMessage;

    private final PostgresWriterProperties properties;

    PostgresWritingRecordParsedItemHandler(
            PostgresWriterProperties properties, RecordParserPostgresConnection postgresConnection) {
        this.properties = properties;
        initSqlStatements(postgresConnection.getConnection());
    }

    @PreDestroy
    public void preDestroy() {
        closeStatements();
    }

    private void initSqlStatements(Connection connection) throws ParserException {
        try {
            sqlInsertTransaction = connection.prepareStatement("INSERT INTO t_transactions"
                    + " (fk_node_acc_id, memo, valid_start_ns, type, fk_payer_acc_id"
                    + ", result, consensus_ns, fk_cud_entity_id, charged_tx_fee"
                    + ", initial_balance, fk_rec_file_id, valid_duration_seconds, max_fee"
                    + ", transaction_hash, transaction_bytes)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            sqlInsertTransferList = connection.prepareStatement("INSERT INTO t_cryptotransferlists"
                    + " (consensus_timestamp, amount, realm_num, entity_num)"
                    + " VALUES (?, ?, ?, ?)");

            sqlInsertNonFeeTransfers = connection.prepareStatement("insert into non_fee_transfers"
                    + " (consensus_timestamp, amount, realm_num, entity_num)"
                    + " values (?, ?, ?, ?)");

            sqlInsertFileData = connection.prepareStatement("INSERT INTO t_file_data"
                    + " (consensus_timestamp, file_data)"
                    + " VALUES (?, ?)");

            sqlInsertContractCall = connection.prepareStatement("INSERT INTO t_contract_result"
                    + " (consensus_timestamp, function_params, gas_supplied, call_result, gas_used)"
                    + " VALUES (?, ?, ?, ?, ?)");

            sqlInsertClaimData = connection.prepareStatement("INSERT INTO t_livehashes"
                    + " (consensus_timestamp, livehash)"
                    + " VALUES (?, ?)");

            sqlInsertTopicMessage = connection.prepareStatement("insert into topic_message"
                    + " (consensus_timestamp, realm_num, topic_num, message, running_hash, sequence_number)"
                    + " values (?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            throw new ParserException("Unable to prepare SQL statements", e);
        }
    }

    @Override
    public void onFileComplete() {
        executeBatches();
    }

    private void closeStatements() {
        try {
            sqlInsertFileData.close();
            sqlInsertTransferList.close();
            sqlInsertNonFeeTransfers.close();
            sqlInsertTransaction.close();
            sqlInsertContractCall.close();
            sqlInsertClaimData.close();
            sqlInsertTopicMessage.close();
        } catch (SQLException e) {
            throw new ParserException("Error closing connection", e);
        }
    }

    /**
     * @throws ParserException if executing the batches fails.
     */
    private void executeBatches() {
        try {
            int[] transactions = sqlInsertTransaction.executeBatch();
            int[] transferLists = sqlInsertTransferList.executeBatch();
            int[] nonFeeTransfers = sqlInsertNonFeeTransfers.executeBatch();
            int[] fileData = sqlInsertFileData.executeBatch();
            int[] contractCalls = sqlInsertContractCall.executeBatch();
            int[] claimData = sqlInsertClaimData.executeBatch();
            int[] topicMessages = sqlInsertTopicMessage.executeBatch();
            log.info("Inserted {} transactions, {} transfer lists, {} files, {} contracts, {} claims, {} topic " +
                            "messages, {} non-fee transfers",
                    transactions.length, transferLists.length, fileData.length, contractCalls.length, claimData.length,
                    topicMessages.length, nonFeeTransfers.length);
            batch_count = 0;
        } catch (SQLException e) {
            log.error("Error committing sql insert batch ", e);
            throw new ParserException(e);
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
            sqlInsertTransaction.setLong(F_TRANSACTION.FK_REC_FILE_ID.ordinal(), 0); // column deprecated, so value 0.
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
