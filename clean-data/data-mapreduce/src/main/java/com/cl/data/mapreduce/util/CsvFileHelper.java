package com.cl.data.mapreduce.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
public final class CsvFileHelper {

    public static CSVParser reader(File file) throws IOException {
        return CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT);
    }

    public static CSVParser reader(String file) throws IOException {
        return CsvFileHelper.reader(new File(file));
    }

    public static CSVParser reader(String file, String[] header) throws IOException {
        return CsvFileHelper.reader(new File(file), header);
    }

    public static CSVParser reader(File file, String[] header) throws IOException {
        return CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withHeader(header));
    }

    public static CSVParser reader(String file, String[] header, boolean skipHeaderRecord) throws IOException {
        return CsvFileHelper.reader(new File(file), header, skipHeaderRecord);
    }

    public static CSVParser reader(File file, String[] header, boolean skipHeaderRecord) throws IOException {
        return CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT.withHeader(header)
            .withSkipHeaderRecord(skipHeaderRecord));
    }

    public static CSVPrinter writer(String file) throws IOException {
        return CsvFileHelper.writer(new File(file));
    }

    public static CSVPrinter writer(File file) throws IOException {
        return CSVFormat.DEFAULT.print(file, StandardCharsets.UTF_8);
    }

    public static CSVPrinter writer(String file, boolean append) throws IOException {
        return CsvFileHelper.writer(new File(file), append);
    }

    public static CSVPrinter writer(File file, boolean append) throws IOException {
        return CSVFormat.DEFAULT.print(new OutputStreamWriter(new FileOutputStream(file, append),
            StandardCharsets.UTF_8));
    }

    public static CSVPrinter writer(String file, String[] header) throws IOException {
        return CsvFileHelper.writer(new File(file), header);
    }

    public static CSVPrinter writer(File file, String[] header) throws IOException {
        return CSVFormat.DEFAULT.withHeader(header).print(file, StandardCharsets.UTF_8);
    }

    public static CSVPrinter writer(String file, String[] header, boolean append) throws IOException {
        return CsvFileHelper.writer(new File(file), header, append);
    }

    public static CSVPrinter writer(File file, String[] header, boolean append) throws IOException {
        if (append) {
            return CsvFileHelper.writer(file, true);
        } else {
            return CsvFileHelper.writer(file, header);
        }
    }

    public static void writerRecord(CSVPrinter writer, CSVRecord record, int... index) throws IOException {
        if (index == null || index.length == 0) {
            writer.printRecord(record);
        } else {
            List<String> values = new ArrayList<>();
            for (int i : index) {
                values.add(record.get(i));
            }
            writer.printRecord(values);
        }
    }

    private CsvFileHelper() {
    }
}
