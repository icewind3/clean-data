package com.cl.graph.weibo.api.manage;

import com.cl.graph.weibo.api.util.CsvFileHelper;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.core.exception.ServiceException;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Component
public class CsvFileManager {

    private static final String SEGMENT_DIRECTORY = "segment";

    private Logger logger = LoggerFactory.getLogger(getClass());

    public void writeToCsvFile(List<List<Object>> content, CsvFile csvFile) throws ServiceException {
        String filePath = csvFile.getFilePath() + File.separator + csvFile.getFileName() + FileSuffixConstants.CSV;

        File file = new File(filePath);
        boolean needAppend = false;
        String[] headers = csvFile.getHeader();
        if (file.exists()) {
            try (CSVParser csvParser = CsvFileHelper.reader(file)) {
                Iterator<CSVRecord> iterator = csvParser.iterator();
                if (iterator.hasNext()) {
                    CSVRecord next = iterator.next();
                    for (int i = 0; i < headers.length; i++) {
                        if (!StringUtils.equals(next.get(i), headers[i])) {
                            throw new ServiceException("header不匹配，无法新增记录");
                        }
                    }
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
            needAppend = true;
        }

        try (CSVPrinter csvFilePrinter = CsvFileHelper.writer(file, headers, needAppend)) {
            for (List<Object> row : content) {
                csvFilePrinter.printRecord(row);
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public void mergeFile(List<String> filePathList, CsvFile resultCsvFile) throws ServiceException {
        if (filePathList == null || filePathList.size() == 0) {
            return;
        }

        String resultFilePath = resultCsvFile.getFilePath();
        String mergeFileName = resultCsvFile.getFileName();
        String[] header = resultCsvFile.getHeader();
        File resultFileDir = new File(resultFilePath);
        if (!resultFileDir.exists()){
            resultFileDir.mkdirs();
        }
        File newFile = new File(resultFilePath + File.separator + mergeFileName + FileSuffixConstants.CSV);

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(newFile, header)) {
            for (String filePath : filePathList) {
                long startTime = System.currentTimeMillis();
                if (logger.isInfoEnabled()) {
                    logger.info("开始读取写入文件{}", filePath);
                }
                try (CSVParser csvParser = CsvFileHelper.reader(filePath, header, true)) {
                    csvPrinter.printRecords(csvParser);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("文件{}读取写入完成，共耗时{}ms", filePath, System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public void mergeFile(CsvFile csvFile, String[] fileNames) throws ServiceException {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }

        String filePath = csvFile.getFilePath();
        String mergeFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        File newFile = new File(filePath + File.separator + mergeFileName + FileSuffixConstants.CSV);

        try (CSVPrinter csvPrinter = CsvFileHelper.writer(newFile, header)) {
            for (String fileName : fileNames) {
                String readFilePath = filePath + File.separator + fileName;
                long startTime = System.currentTimeMillis();
                if (logger.isInfoEnabled()) {
                    logger.info("开始读取写入文件{}", readFilePath);
                }
                try (CSVParser csvParser = CsvFileHelper.reader(readFilePath, header, true)) {
                    csvPrinter.printRecords(csvParser);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("文件{}读取写入完成，共耗时{}ms", readFilePath, System.currentTimeMillis() - startTime);
                }
            }
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }

    public void segmentFile(CsvFile csvFile, String[] fileNames, int segmentSize) throws ServiceException {
        if (fileNames == null || fileNames.length == 0) {
            return;
        }
        String filePath = csvFile.getFilePath();
        String newFileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        String segmentPath = filePath + File.separator + SEGMENT_DIRECTORY;
        File segmentFile = new File(segmentPath);
        segmentFile.mkdir();
        int index = 0;
        int count = 0;
        CSVPrinter csvPrinter = null;
        long startTime = System.currentTimeMillis();
        try {
            File newFile = null;
            for (String fileName : fileNames) {
                try (CSVParser csvParser = CsvFileHelper.reader(filePath + File.separator + fileName, header,
                        true)) {
                    for (CSVRecord record : csvParser) {
                        if (csvPrinter == null || count >= segmentSize) {
                            if (csvPrinter != null) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                                }
                                csvPrinter.close();
                            }
                            count = 0;
                            newFile = new File(segmentPath + File.separator + newFileName + index + FileSuffixConstants.CSV);
                            index++;
                            csvPrinter = CsvFileHelper.writer(newFile, header);
                            if (logger.isInfoEnabled()) {
                                startTime = System.currentTimeMillis();
                                logger.info("开始写入分割文件{}", newFile.getPath());
                            }
                        }
                        csvPrinter.printRecord(record);
                        count++;
                    }
                } catch (IOException e) {
                    throw new ServiceException(e);
                }
            }
        } finally {
            if (csvPrinter != null) {
                try {
                    csvPrinter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void segmentFile(CsvFile csvFile, int segmentSize) throws ServiceException {
        String filePath = csvFile.getFilePath();
        String fileName = csvFile.getFileName();
        String[] header = csvFile.getHeader();
        String segmentPath = filePath + File.separator + SEGMENT_DIRECTORY;
        File segmentFile = new File(segmentPath);
        segmentFile.mkdir();
        int index = 0;
        int count = 0;
        long startTime = System.currentTimeMillis();
        File oriFile = new File(filePath + File.separator + fileName);
        boolean hasHeader = header != null && header.length > 0;
        CSVPrinter csvPrinter = null;
        try {
            File newFile = null;
            try (CSVParser csvParser = CsvFileHelper.reader(oriFile, header, hasHeader)) {
                for (CSVRecord record : csvParser) {
                    if (csvPrinter == null || count >= segmentSize) {
                        if (csvPrinter != null) {
                            if (logger.isInfoEnabled()) {
                                logger.info("分割写入文件{}完成，耗时{}ms", newFile.getPath(), System.currentTimeMillis() - startTime);
                            }
                            csvPrinter.close();
                        }
                        count = 0;
                        newFile = new File(segmentPath + File.separator + fileName + index + FileSuffixConstants.CSV);
                        index++;
                        if (hasHeader){
                            csvPrinter = CsvFileHelper.writer(newFile, header);
                        }else {
                            csvPrinter = CsvFileHelper.writer(newFile);
                        }
                        if (logger.isInfoEnabled()) {
                            startTime = System.currentTimeMillis();
                            logger.info("开始写入分割文件{}", newFile.getPath());
                        }
                    }
                    csvPrinter.printRecord(record);
                    count++;
                }
            } catch (IOException e) {
                throw new ServiceException(e);
            }
        } finally {
            if (csvPrinter != null) {
                try {
                    csvPrinter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
