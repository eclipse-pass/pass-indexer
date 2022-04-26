package org.dataconservancy.pass.indexer.reindex;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import org.dataconservancy.pass.model.PassEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqliteRunner implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SqliteRunner.class);

    public static final int PROGRESS_FAILED = -1;
    public static final int PROGRESS_ENQUEUED = 0;
    public static final int PROGRESS_RUNNING = 1;
    public static final int PROGRESS_DONE = 100;

    static final String STMNT_CREATE_TABLE_RESULTS = "CREATE TABLE results ("
            + "id INTEGER PRIMARY KEY, url TEXT NOT NULL, type TEXT NOT NULL, "
            + "status INTEGER NOT NULL, result TEXT)";

    static final String STMNT_CREATE_TABLE_TYPE_QUEUE = String.format(
            "CREATE TABLE types_queue ("
                    + "id INTEGER PRIMARY KEY, type TEXT NOT NULL, progress INTEGER NOT NULL DEFAULT %d)",
                    PROGRESS_ENQUEUED);

    static final String STMNT_CREATE_TABLE_ITEM_QUEUE = String.format("CREATE TABLE item_queue ("
            + "id INTEGER PRIMARY KEY, type TEXT NOT NULL, url TEXT NOT NULL, progress INTEGER NOT NULL DEFAULT %d)",
            PROGRESS_ENQUEUED);

    static final String STMNT_ITEM_START = String.format("UPDATE item_queue SET progress = %s WHERE id = ?",
            PROGRESS_RUNNING);
    static final String STMNT_ITEM_REMOVE = String.format("DELETE FROM item_queue WHERE id = ?");
    static final String STMNT_ITEM_FAIL = String.format("UPDATE item_queue SET progress = %d WHERE id = ?",
            PROGRESS_FAILED);
    static final String STMNT_ITEM_SAVE_RESULT = "INSERT INTO results (type, url, status, result) VALUES (?, ?, ?, ?)";
    static final String STMNT_ITEM_POLL = String
            .format("SELECT id, type, url FROM item_queue WHERE progress = %d LIMIT ?", PROGRESS_ENQUEUED);

    static final String STMNT_FAIL_COUNT_TYPES = String.format("SELECT count(*) FROM  types_queue WHERE progress = %d",
            PROGRESS_FAILED);
    static final String STMNT_FAIL_COUNT_ITEMS = String.format("SELECT count(*) FROM item_queue WHERE PROGRESS = %d",
            PROGRESS_FAILED);

    private final Connection conn;
    private final String filepath;

    // Default 4 threads
    BlockingExecutor exe = new BlockingExecutor(4);

    public SqliteRunner(Collection<Class<? extends PassEntity>> types) {
        this.filepath = newFilePath();
        conn = open(this.filepath);
        init(conn);

        try {
            begin();
            for (Class<?> t : types) {
                execUpdate("INSERT INTO types_queue (type) VALUES (?);", stmnt -> {
                    stmnt.setString(1, t.getName());
                });
            }
            commit();
        } catch (Exception e) {
            quietly(() -> {
                conn.close();
            });

            try {
                Files.delete(new File(filepath).toPath());
            } catch (Exception x) {
                if (Files.exists(new File(filepath).toPath())) {
                    LOG.warn("Could not delete db file at {}", filepath);
                }
            }

            throw new RuntimeException("could not initialize", e);
        }
    }

    public SqliteRunner(String filepath) {
        this.filepath = filepath;
        conn = open(filepath);
    }

    public String getFilePath() {
        return this.filepath;
    }

    public void process(Function<Class<? extends PassEntity>, Stream<URI>> lister, Function<URI, String> task) {

        // Listing by type and enqueueing, one-by-one so that Fedora isn't killed.
        exe.execute(() -> {
            try {
                for (Class<? extends PassEntity> type = nextType(); type != null; type = nextType()) {
                    final Class<? extends PassEntity> next = type;
                    LOG.info("Enqueueing " + type.getSimpleName());
                    enqueue(next, lister);
                    LOG.info("Done enqueueing " + type.getSimpleName());
                }
            } catch (Exception e) {
                LOG.warn("Error populating item queue", e);
            }
        });

        // Keep processing items until there are no more
        while (!isDoneQueueing()) {
            processItems(task);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted");
                return;
            }
        }

        // last round of processing, now that we know we're done populating the queues.
        processItems(task);
        exe.awaitDone();
    }

    private void processItems(Function<URI, String> task) {
        AtomicInteger processedCount = new AtomicInteger();
        for (Item item : queuedItems()) {
            exe.execute(() -> {
                try {
                    item.process(task);
                    if (processedCount.incrementAndGet() % 1000 == 0) {
                        LOG.info("Processed " + processedCount.get());
                    }
                } catch (Exception e) {
                    LOG.warn("Uncaut exception processing items", e);
                }
            });
        }
    }

    private Iterable<Item> queuedItems() {
        return () -> new Iterator<Item>() {

            Queue<Item> items = new ConcurrentLinkedQueue<Item>();

            @Override
            public boolean hasNext() {
                if (items.isEmpty()) {
                    items.addAll(fetchItems(100));
                }
                return !items.isEmpty();
            }

            @Override
            public Item next() {
                hasNext();
                return items.poll();
            }
        };
    }

    private Collection<Item> fetchItems(int count) {
        List<Item> items = new ArrayList<Item>(count);
        synchronized (conn) {
            autocommit();

            execQuery(STMNT_ITEM_POLL, stmnt -> {
                stmnt.setInt(1, count);
            }, results -> {
                while (results.next()) {
                    items.add(new Item(results.getInt(1), results.getString(2), results.getString(3)).start(conn));
                }
            });
        }

        return items;
    }

    private void begin() {
        quietly(() -> {
            conn.setAutoCommit(false);
        });
    }

    private void rollback() {
        quietly(() -> {
            conn.rollback();
        });
    }

    private void commit() {
        quietly(() -> {
            conn.commit();
        });
    }

    public void autocommit() {
        quietly(() -> {
            conn.setAutoCommit(true);
        });
    }

    /**
     * Returns true if the runner is done populating the queue of items to process.
     */
    public boolean isDoneQueueing() {
        synchronized (conn) {

            AtomicBoolean isDone = new AtomicBoolean();

            autocommit();

            // Count from types queue
            execQuery(String.format("SELECT count(*) from types_queue WHERE progress = %d;", PROGRESS_ENQUEUED), null,
                    results -> {
                        isDone.set(results.next() && results.getInt(1) == 0);
                    });

            return isDone.get();
        }
    }

    public int errorCount() {
        AtomicInteger count = new AtomicInteger(0);
        synchronized (conn) {
            autocommit();

            // Count from types queue
            execQuery(STMNT_FAIL_COUNT_TYPES, null, results -> {
                if (results.next()) {
                    count.getAndAdd(results.getInt(1));
                }
            });

            // count from items queue
            execQuery(STMNT_FAIL_COUNT_ITEMS, null, results -> {
                if (results.next()) {
                    count.getAndAdd(results.getInt(1));
                }
            });
        }

        return count.get();
    }

    public void clearErrors() {
        synchronized (conn) {
            autocommit();

            execUpdate(String.format("UPDATE types_queue SET progress = %d WHERE progress = %d", PROGRESS_ENQUEUED,
                    PROGRESS_FAILED), null);
            execUpdate(String.format("UPDATE item_queue SET progress = %d WHERE progress = %d", PROGRESS_ENQUEUED,
                    PROGRESS_FAILED), null);
        }
    }

    private interface SQLRunnable {
        public void run() throws SQLException;
    }

    private void quietly(SQLRunnable stmnt) {
        try {
            stmnt.run();
        } catch (SQLException e) {
            throw new RuntimeException("Error performing quiet SQL operation", e);
        }
    }

    private interface SQLConsumer<T> {
        public void accept(T t) throws SQLException;
    }

    int execUpdate(String query, SQLConsumer<PreparedStatement> statementHandler) {

        try (PreparedStatement q = conn.prepareStatement(query)) {
            if (statementHandler != null) {
                statementHandler.accept(q);
            }

            return q.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL", e);
        }
    }

    private void execQuery(String query, SQLConsumer<PreparedStatement> statementHandler,
            SQLConsumer<ResultSet> resultHandler) {

        try (PreparedStatement q = conn.prepareStatement(query)) {
            if (statementHandler != null) {
                statementHandler.accept(q);
            }

            try (ResultSet r = q.executeQuery()) {
                if (resultHandler != null) {
                    resultHandler.accept(r);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Could not execute SQL", e);
        }
    }

    private class Item {
        public int id;
        public String uri;
        public String type;

        public Item(int id, String type, String uri) {
            this.id = id;
            this.type = type;
            this.uri = uri;
        }

        public Item start(Connection conn) {
            try {
                begin();
                execUpdate(STMNT_ITEM_START, s -> {
                    s.setInt(1, this.id);
                });
                commit();
            } catch (Exception e) {
                rollback();
                throw new RuntimeException("could not take item out of queue", e);
            }
            return this;
        }

        private void saveFailure(Exception e) {
            synchronized (conn) {
                try {
                    begin();

                    // Set to failed state
                    execUpdate(STMNT_ITEM_FAIL, s -> {
                        s.setInt(1, this.id);
                    });

                    // Save failure, for in case we need forensics
                    execUpdate(STMNT_ITEM_SAVE_RESULT, s -> {
                        s.setString(1, this.type);
                        s.setString(2, this.uri);
                        s.setInt(3, PROGRESS_FAILED);
                        s.setString(4, getStackTrace(e));
                    });

                    commit();
                } catch (Exception x) {
                    rollback();
                    LOG.warn("Could not save the following error: \n" + getStackTrace(e), x);
                }
            }
        }

        public boolean process(Function<URI, String> task) {
            final String result;
            try {
                result = task.apply(new URI(this.uri));
            } catch (Exception e) {
                saveFailure(e);
                return false;
            }

            // Success path
            synchronized (conn) {
                try {
                    begin();

                    // Remove from queue
                    execUpdate(STMNT_ITEM_REMOVE, s -> {
                        s.setInt(1, this.id);
                    });

                    // Save result, for in case we need to re-populate the index
                    execUpdate(STMNT_ITEM_SAVE_RESULT, s -> {
                        s.setString(1, this.type);
                        s.setString(2, this.uri);
                        s.setInt(3, PROGRESS_DONE);
                        s.setString(4, result);
                    });

                    commit();
                } catch (Exception e) {
                    rollback();
                    LOG.warn("Could not save success result, will retry later", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * Select next type for processing, updates progress to 1 in the db for the
     * given type
     */
    @SuppressWarnings("unchecked")
    private Class<? extends PassEntity> nextType() {

        final AtomicReference<String> type = new AtomicReference<String>();
        final Class<? extends PassEntity> entity;
        synchronized (conn) {

            autocommit();
            execQuery("SELECT type, id FROM types_queue WHERE progress = 0 LIMIT 1", null, r -> {
                if (r.next()) {
                    type.set(r.getString(1));
                }
            });

            if (type.get() == null) {
                return null;
            }

            try {
                begin();
                entity = (Class<? extends PassEntity>) Class.forName(type.get());
                execUpdate(String.format("UPDATE types_queue SET progress = %s WHERE type = '%s'", PROGRESS_RUNNING,
                        type.get()), null);
                commit();
            } catch (Exception e) {
                rollback();
                throw new RuntimeException("Could not update type queue", e);
            }
        }

        return entity;
    }

    private void enqueue(Class<? extends PassEntity> type,
            Function<Class<? extends PassEntity>, Stream<URI>> lister) {

        synchronized (conn) {
            try {
                begin();

                lister.apply(type).forEach(url -> {
                    execUpdate("INSERT INTO item_queue (type, url) VALUES (?, ?)", s -> {
                        s.setString(1, type.getSimpleName());
                        s.setString(2, url.toString());
                    });
                });

                execUpdate(String.format("UPDATE types_queue SET progress = %d WHERE type = '%s'", PROGRESS_DONE,
                        type.getName()),
                        null);

                commit();
            } catch (Exception e) {
                rollback();
                autocommit();
                execUpdate(String.format("UPDATE types_queue SET progress = %d WHERE type = '%s'", PROGRESS_FAILED,
                        type.getName()), null);
                LOG.warn("error loading item queue", e);
            }
        }

    }

    private Connection open(String filepath) {
        try {
            return DriverManager.getConnection("jdbc:sqlite:" + filepath);
        } catch (SQLException e) {
            throw new RuntimeException("Failed opening db at " + filepath, e);
        }
    }

    private String newFilePath() {
        DateFormat date = new SimpleDateFormat("yyyy_mm_dd-HHMMss.SSS");
        return date.format(new Date()) + ".db";
    }

    private void init(Connection c) {
        begin();
        execUpdate(STMNT_CREATE_TABLE_TYPE_QUEUE, null);
        execUpdate(STMNT_CREATE_TABLE_ITEM_QUEUE, null);
        execUpdate(STMNT_CREATE_TABLE_RESULTS, null);
        commit();
    }

    private static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    @Override
    public void close() {
        quietly(() -> conn.close());
    }
}
