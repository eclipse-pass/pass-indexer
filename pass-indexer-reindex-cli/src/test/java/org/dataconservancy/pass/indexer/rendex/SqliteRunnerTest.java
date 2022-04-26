package org.dataconservancy.pass.indexer.rendex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.dataconservancy.pass.indexer.reindex.Main;
import org.dataconservancy.pass.indexer.reindex.SqliteRunner;
import org.dataconservancy.pass.model.Submission;
import org.dataconservancy.pass.model.SubmissionEvent;
import org.junit.Test;

public class SqliteRunnerTest {

    URI uri1 = URI.create("test:/1");
    URI uri2 = URI.create("test:/2");
    URI uri3 = URI.create("test:/3");
    URI uri4 = URI.create("test:/4");

    @Test
    public void basicTest() throws Exception {

        SqliteRunner runner = new SqliteRunner(Main.PASS_TYPES);
        Collection<URI> processed = run(runner, 0, 0);

        assertEquals(4, processed.size());
        assertTrue(processed.containsAll(Arrays.asList(uri1, uri2, uri3, uri4)));
        assertEquals(0, runner.errorCount());

        runner.close();
        Files.delete(new File(runner.getFilePath()).toPath());
    }

    @Test
    public void basicResumptionTest() throws Exception {
        SqliteRunner runner = new SqliteRunner(Main.PASS_TYPES);
        runner.close();

        SqliteRunner reopen = new SqliteRunner(runner.getFilePath());
        Collection<URI> processed = run(reopen, 0, 0);

        assertEquals(4, processed.size());
        assertTrue(processed.containsAll(Arrays.asList(uri1, uri2, uri3, uri4)));
        assertEquals(0, reopen.errorCount());

        reopen.close();
        Files.delete(new File(runner.getFilePath()).toPath());
    }

    @Test
    public void basiclistingErrorTest() throws Exception {
        SqliteRunner runner = new SqliteRunner(Main.PASS_TYPES);
        Collection<URI> processed = run(runner, 1, 0);

        assertEquals(2, processed.size());
        assertEquals(1, runner.errorCount());

        runner.close();
        Files.delete(new File(runner.getFilePath()).toPath());
    }

    @Test
    public void basicItemErrorTest() throws Exception {
        SqliteRunner runner = new SqliteRunner(Main.PASS_TYPES);
        Collection<URI> processed = run(runner, 0, 3);

        assertEquals(1, processed.size());
        assertEquals(3, runner.errorCount());

        runner.close();
        Files.delete(new File(runner.getFilePath()).toPath());
    }

    @Test
    public void resumeErrorTest() throws Exception {

        // Only one item should be processed, given our expected errors
        // We should see our two errors in the error count.
        SqliteRunner runner = new SqliteRunner(Main.PASS_TYPES);
        assertEquals(1, run(runner, 1, 1).size());
        assertEquals(2, runner.errorCount());
        runner.close();

        // Reopening shouldn't change the error count, and running should not
        // actually run anything (since errored items are skipped)
        SqliteRunner reopen = new SqliteRunner(runner.getFilePath());
        assertEquals(2, reopen.errorCount());
        assertEquals(0, run(reopen, 0, 0).size());
        assertEquals(2, reopen.errorCount());

        // Clearing errors should clear errors
        reopen.clearErrors();
        assertEquals(0, reopen.errorCount());

        // Now, with only one item expected to fail, we should process two more items
        // and see only one error;
        assertEquals(2, run(reopen, 0, 1).size());
        assertEquals(1, reopen.errorCount());

        // clearing errors and running again should pick up the last result
        reopen.clearErrors();
        assertEquals(0, reopen.errorCount());
        assertEquals(1, run(reopen, 0, 0).size());
        assertEquals(0, reopen.errorCount());

        reopen.close();
        Files.delete(new File(runner.getFilePath()).toPath());
    }

    private Collection<URI> run(SqliteRunner runner, int listingFailureCount, int itemFailureCount) {

        List<URI> processed = new ArrayList<URI>();

        Counter listingFailures = new Counter(listingFailureCount);
        Counter itemFailures = new Counter(itemFailureCount);

        runner.process(e -> {

            // We only enqueue four items, from two containers
            if (e.equals(Submission.class)) {
                if (listingFailures.hasMore()) {
                    System.out.println("Throwing exception");
                    throw new RuntimeException("expected failure");
                }
                return Arrays.asList(uri1, uri2).stream();
            }

            if (e.equals(SubmissionEvent.class)) {
                if (listingFailures.hasMore()) {
                    throw new RuntimeException("expected failure");
                }
                return Arrays.asList(uri3, uri4).stream();
            }

            return Stream.empty();
        }, uri -> {

            if (itemFailures.hasMore()) {
                throw new RuntimeException("expected failure");
            }
            processed.add(uri);
            return uri.toString();
        });

        return processed;
    }

    private class Counter {
        int count;

        Counter(int c) {
            count = c;
        }

        public boolean hasMore() {
            return count-- > 0;
        }
    }
}
