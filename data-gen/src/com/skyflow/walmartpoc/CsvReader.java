package com.skyflow.walmartpoc;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class CsvReader<T extends SerializableDeserializable> implements AutoCloseable, Iterable<T> {
    private final CSVReader reader;
        private final Method fromCsvRecordMethod;
    
        public CsvReader(Class<T> clazz, Path path, String[] header) throws IOException, CsvValidationException {
            this.reader = new CSVReader(new FileReader(path.toFile()));
            if (header != null) {
                String[] fileHeader = reader.readNext();
                if (fileHeader == null || fileHeader.length != header.length) {
                    throw new RuntimeException("CSV header does not match the expected header.");
                }
                for (int i = 0; i < header.length; i++) {
                    if (!fileHeader[i].equals(header[i])) {
                        throw new RuntimeException(String.format("CSV header does not match the expected header at location %d: expected %s got %s.", i, header[i], fileHeader[i]));
                    }
                }
            }
            try {
                this.fromCsvRecordMethod = clazz.getMethod("fromCsvRecord", String[].class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("The class " + clazz.getName() + " does not have a static method fromCsvRecord(String[]).", e);
            }
    }

    @Override
    public Iterator<T> iterator() {
        try {
            return new Iterator<T>() {
                private String[] nextLine = reader.readNext();

                @Override
                public boolean hasNext() {
                    return nextLine != null;
                }

                @SuppressWarnings("unchecked")
                @Override
                public T next() {
                    if (nextLine == null) {
                        throw new NoSuchElementException();
                    }
                    T obj;
                    try {
                        obj = (T) fromCsvRecordMethod.invoke(null, (Object) nextLine);
                    } catch (Exception e) {
                        throw new RuntimeException("Error invoking fromCsvRecord method", e);
                    }
                    try {
                        nextLine = reader.readNext();
                    } catch (IOException | CsvValidationException e) {
                        throw new RuntimeException("Error reading next line from CSV", e);
                    }
                    return obj;
                }
            };
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException("Error occurred while creating CSV iterator", e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    public static <T extends SerializableDeserializable> T[] readCsvFile(Class<T> clazz, Path path, String[] csvHeader ) throws CsvValidationException, IOException {
    try (CsvReader<T> csvReader = new CsvReader<T>(clazz, path, csvHeader);) {
        List<T> l = new ArrayList<>(1000000); // Horribly inefficient way to do this. Works for our small scale though.
        for (T c : csvReader) {
            l.add(c);
        }
        @SuppressWarnings("unchecked")
        T[] a = (T[]) l.toArray((T[]) java.lang.reflect.Array.newInstance(clazz, 0));
        return a;
    }
}

}
