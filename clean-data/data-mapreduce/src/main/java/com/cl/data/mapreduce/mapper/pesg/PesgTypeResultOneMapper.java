package com.cl.data.mapreduce.mapper.pesg;

import com.cl.data.mapreduce.constant.WordSegmentationConstants;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/28
 */
public class PesgTypeResultOneMapper extends BasePesgTypeResultMapper {

    private static final Set<String> UID_SET = new HashSet<>();

    @Override
    protected Set<String> getUidSet() {
        return UID_SET;
    }

    @Override
    protected String getType(){
        return WordSegmentationConstants.TYPE_RESULT_1;
    }

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        Configuration conf = context.getConfiguration();
//        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
//        if (uidUris == null){
//            return;
//        }
//        for (URI uidUri : uidUris) {
//            Path uidPath = new Path(uidUri.getPath());
//            String uidFileName = uidPath.getName();
//            initSet(UID_SET, uidFileName);
//        }
//    }
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        CSVParser parse = CSVParser.parse(value.toString(), CSVFormat.DEFAULT);
//        for (CSVRecord record : parse){
//            String uid = record.get(0);
//            if ("uid".equals(uid)){
//                continue;
//            }
//            if (UID_SET.size() > 0 && !UID_SET.contains(uid)){
//                return;
//            }
//            context.write(new Text(uid), new TopInput(new Text(WordSegmentationConstants.TYPE_RESULT_1), value));
//        }
//    }
//
//    private static void initSet(Set<String> set, String fileName) {
//        try {
//            BufferedReader fis = new BufferedReader(new FileReader(fileName));
//            String uid;
//            while ((uid = fis.readLine()) != null) {
//                set.add(uid);
//            }
//        } catch (IOException ioe) {
//            System.err.println("Caught exception while parsing the cached file '"
//                + org.apache.hadoop.util.StringUtils.stringifyException(ioe));
//        }
//    }
}
