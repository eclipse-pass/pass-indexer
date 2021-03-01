package org.dataconservancy.pass.indexer.reindex;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ResultStager implements Consumer<URI> {

    final File file;
    final PrintWriter writer;

    public ResultStager() {
        try {
            file = Files.createTempFile(".reseults", null).toFile();
            file.deleteOnExit();
            writer = new PrintWriter(file);
        } catch (Exception e) {
            throw new RuntimeException("Could not create temp file", e);
        }

    }

    @Override
    public void accept(URI val) {
        writer.println(val);
    }

    Stream<URI> entries() {
        try {
            writer.close();
            writer.flush();
            return Files.lines(file.toPath()).map(URI::create);
        } catch (IOException e) {
            throw new RuntimeException("could not read back temp file", e);
        }
    }
}
