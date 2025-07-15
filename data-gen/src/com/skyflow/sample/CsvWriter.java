package com.skyflow.sample;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import com.opencsv.CSVWriter;

public class CsvWriter<T extends SerializableDeserializable> implements AutoCloseable {
    private final CSVWriter writer;

    public CsvWriter(Path path, String[] header) throws IOException {
        this.writer = new CSVWriter(new FileWriter(path.toFile()));
        if (header != null) {
            writer.writeNext(header);
        }
    }

    public void write(T obj) {
        try {
            writer.writeNext(obj.toCsvRecord());
        } catch (Exception e) {
            throw new RuntimeException("Error writing object to CSV", e);
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
