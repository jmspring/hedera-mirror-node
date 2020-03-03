package com.hedera.mirror.importer.parser.record;

import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.PreDestroy;
import javax.inject.Named;
import javax.sql.DataSource;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

/**
 * Connection to PostgreSQL shared by {@link RecordFileParser} and {@link PostgresWritingRecordParsedItemHandler}.
 * RecordFileParser writes info about file and manages complete transaction commit/rollback.
 * PostgresWritingRecordParsedItemHandler writes transactions, transfer list, topic messages, etc.
 * Rather than either of them owning the connection and passing it to other, via what are right now database agnostic
 * interfaces, this way of injecting common connection to both seems a good way.
 */
@Log4j2
@Named
public class RecordParserPostgresConnection {
    @Getter
    private final Connection connection;

    public RecordParserPostgresConnection(DataSource dataSource) {
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);  // do not auto-commit
        } catch (SQLException e) {
            log.error("Unable to setup connection to database");  // TODO: update logs in documentation?
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void preDestroy() throws SQLException {
        connection.close();
    }
}
