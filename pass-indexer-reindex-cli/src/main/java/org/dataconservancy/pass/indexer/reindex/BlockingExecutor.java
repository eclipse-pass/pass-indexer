package org.dataconservancy.pass.indexer.reindex;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockingExecutor implements Executor {

    Logger LOG = LoggerFactory.getLogger(BlockingExecutor.class);

    final ExecutorService exe;
    final Semaphore worker;
    final int nthreads;

    public BlockingExecutor(int nthreads) {
        this.nthreads = nthreads;
        exe = Executors.newFixedThreadPool(nthreads);
        worker = new Semaphore(nthreads);
    }

    @Override
    public void execute(Runnable command) {
        try {
            worker.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        exe.execute(() -> {
            try {
                command.run();
            } catch (Throwable t) {
                LOG.warn("Thread terminaed with error", t);
            } finally {
                worker.release();
            }
        });
    }

    public void awaitDone() {
        try {
            while (true) {
                if (worker.tryAcquire(nthreads, 0, TimeUnit.SECONDS)) {
                    worker.release(nthreads);
                    return;
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            exe.shutdown();
            Thread.currentThread().interrupt();
            throw new RuntimeException("Execution interrupted", e);
        }
    }

}
