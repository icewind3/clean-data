package com.cl.data.mapreduce.util;

import com.csvreader.CsvReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FileUtil {
    public static Set<String> get205w(String...filepaths) throws Exception {
        Set<String> set = new HashSet<>();
        for(String filepath :filepaths) {
            set.addAll(getColumn(filepath));
        }
        return set;
    }

    public static Set<String> getColumn(String filepath) throws IOException {
        Set<String> set = new HashSet<>();
        CsvReader reader = new CsvReader(filepath, ',', Charset.forName("utf-8"));
        while(reader.readRecord()) {
            set.add(reader.get(0));
        }
        reader.close();
        return set;
    }

    public static List<String> translateCsvValues(Text value) throws IOException {
        Reader in = new StringReader(value.toString());
        CSVParser parser = new CSVParser(in, CSVFormat.EXCEL);
        List<CSVRecord> list = parser.getRecords();
        List<String> valueList = new ArrayList<>();
        for(int i = 0; i < list.get(0).size(); i++) {
            valueList.add(list.get(0).get(i));
        }

        return valueList;
    }

}
