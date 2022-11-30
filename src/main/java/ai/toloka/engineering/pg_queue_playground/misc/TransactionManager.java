package ai.toloka.engineering.pg_queue_playground.misc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TransactionManager {

    private static final Logger logger = LogManager.getLogger();

    private final DataSource dataSource;
    private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();

    public TransactionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static void closeSafely(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.error("Unexpected exception while closing", e);
        }
    }

    public void begin() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connectionThreadLocal.set(connection);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            connectionThreadLocal.remove();
            closeSafely(connection);
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            throw new IllegalStateException("There is no connection to commit");
        }
        try {
            connection.commit();
            connection.close();
        } catch (Exception e) {
            closeSafely(connection);
            throw new RuntimeException(e);
        } finally {
            connectionThreadLocal.remove();
        }
    }

    public void rollbackSafely() {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            logger.warn("Looks like already rolled-back");
            return;
        }
        try {
            connection.rollback();
            connection.close();
        } catch (Exception e) {
            closeSafely(connection);
            throw new RuntimeException(e);
        } finally {
            connectionThreadLocal.remove();
        }
    }

    public void execute(String sql,
                        PreparedStatementConsumer preparedStatementConsumer,
                        ResultSetConsumer resultSetConsumer) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            throw new IllegalStateException("There is no connection to execute");
        }
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatementConsumer.consume(preparedStatement);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSetConsumer.consume(resultSet);
            closeSafely(resultSet);
            closeSafely(preparedStatement);
        } catch (Exception e) {
            rollbackSafely(connection, preparedStatement);
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql, ResultSetConsumer resultSetConsumer) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            throw new IllegalStateException("There is no connection to execute");
        }
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.consume(resultSet);
            closeSafely(resultSet);
            closeSafely(statement);
        } catch (Exception e) {
            rollbackSafely(connection, statement);
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql, PreparedStatementConsumer preparedStatementConsumer) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            throw new IllegalStateException("There is no connection to execute");
        }
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatementConsumer.consume(preparedStatement);
            preparedStatement.execute();
            closeSafely(preparedStatement);
        } catch (Exception e) {
            rollbackSafely(connection, preparedStatement);
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql) {
        Connection connection = connectionThreadLocal.get();
        if (connection == null) {
            throw new IllegalStateException("There is no connection to execute");
        }
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
            closeSafely(statement);
        } catch (Exception e) {
            rollbackSafely(connection, statement);
            throw new RuntimeException(e);
        }
    }

    private void rollbackSafely(Connection connection, Statement statement) {
        try {
            connectionThreadLocal.remove();
            closeSafely(statement);
            connection.rollback();
            closeSafely(connection);
        } catch (Exception e) {
            logger.error("Unexpected exception while rollback and cleanup", e);
            closeSafely(connection);
        }
    }

    public void executeWithoutTx(String sql) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSafely(statement);
            closeSafely(connection);
        }
    }

    public void executeWithoutTx(String sql, ResultSetConsumer resultSetConsumer) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            resultSetConsumer.consume(resultSet);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            closeSafely(resultSet);
            closeSafely(statement);
            closeSafely(connection);
        }
    }

    public interface PreparedStatementConsumer {

        void consume(PreparedStatement ps) throws SQLException;
    }

    public interface ResultSetConsumer {

        void consume(ResultSet rs) throws SQLException;
    }
}
