package com.ecommerce.rt.sink;

import com.ecommerce.rt.model.LateEvent;
import com.ecommerce.rt.window.WindowUtils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Audit sink for events that arrived after the 24h allowed lateness.
 *
 * <p>Rows are upserted by {@code event_id}; the payload keeps the original JSON verbatim.
 * Acceptance requires this table to stay empty.
 */
public class LateJdbcSink extends RichSinkFunction<LateEvent> {

    private static final long serialVersionUID = 1L;

    static final String SQL =
            "INSERT INTO rt_late_events "
                    + "(event_id, event_time_utc, detected_at, lateness_ms, reason, payload) "
                    + "VALUES (?, ?, ?, ?, ?, ?) "
                    + "ON DUPLICATE KEY UPDATE "
                    + " event_time_utc = VALUES(event_time_utc), detected_at = VALUES(detected_at), "
                    + " lateness_ms = VALUES(lateness_ms), reason = VALUES(reason), "
                    + " payload = VALUES(payload)";

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection =
                DriverManager.getConnection(
                        RtJdbcSink.resolveUrl(),
                        RtJdbcSink.resolveUser(),
                        RtJdbcSink.resolvePassword());
        connection.setAutoCommit(false);
    }

    @Override
    public void invoke(LateEvent event, Context context) throws Exception {
        if (connection == null || connection.isClosed()) {
            throw new SQLException("realtime MySQL connection is closed");
        }
        try (PreparedStatement ps = connection.prepareStatement(SQL)) {
            ps.setString(1, event.eventId);
            ps.setObject(2, WindowUtils.utcDateTime(event.eventTime));
            ps.setObject(3, WindowUtils.utcDateTime(event.detectedAt));
            ps.setLong(4, event.latenessMs);
            ps.setString(5, event.reason);
            ps.setString(6, event.payload);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException exc) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
                // Surface the original error.
            }
            throw exc;
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } finally {
                connection = null;
            }
        }
        super.close();
    }
}
