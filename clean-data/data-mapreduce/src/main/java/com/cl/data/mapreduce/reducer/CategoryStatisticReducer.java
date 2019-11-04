package com.cl.data.mapreduce.reducer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.cl.data.mapreduce.bean.LabelCategory;
import com.cl.data.mapreduce.util.BuildCsvString;
import com.cl.data.mapreduce.util.GsonUtil;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

public class CategoryStatisticReducer extends Reducer<Text, Text, Text, NullWritable> {

    private LabelCategory productCategory;
    private LabelCategory brandCategory;
    private LabelCategory starCategoryEn;
    private LabelCategory starCategoryZh;
    private LabelCategory appCategory;
    private LabelCategory locationCategory;
    private LabelCategory movieCategory;
    private LabelCategory musicCategory;
    private boolean flag = true;

    private static final Log logger = LogFactory.getLog(CategoryStatisticReducer.class);

    private MultipleOutputs<Text, NullWritable> mos;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("hello world");
        logger.info("Hello world");
        logger.error("hello world");
        mos = new MultipleOutputs<>(context);
        Configuration configuration = context.getConfiguration();
        productCategory = GsonUtil.getGson().fromJson(configuration.get("product.category"), LabelCategory.class);
        brandCategory = GsonUtil.getGson().fromJson(configuration.get("brand.category"),
                LabelCategory.class);
        starCategoryZh = GsonUtil.getGson().fromJson(configuration.get("star.zh.category"),
                LabelCategory.class);
        starCategoryEn = GsonUtil.getGson().fromJson(configuration.get("star.en.category"),
                LabelCategory.class);
        appCategory = GsonUtil.getGson().fromJson(configuration.get("app.category"),
                LabelCategory.class);
        locationCategory = GsonUtil.getGson().fromJson(configuration.get("city.category"),
                LabelCategory.class);
        movieCategory = GsonUtil.getGson().fromJson(configuration.get("movieCategory.category"),
                LabelCategory.class);
        musicCategory = GsonUtil.getGson().fromJson(configuration.get("music.category"),
                LabelCategory.class);

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        if (flag) {
//            System.out.println("------------------------" + key);
//            logger.error("hellow orld0");
//            logger.info("hklkljlj");
//        }
        Set<String> set = new HashSet<>();
        List<Map<String, Long>> mapList = new ArrayList<>();
        List<Map<String, Long>> mapList2 = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            mapList.add(new HashMap<>());
            mapList2.add(new HashMap<>());
        }
        for(Text text : values) {
            String value = text.toString();
            List<String> labels = JSON.parseArray(value, String.class);
            if (set.add(labels.get(1))) {
                process(mapList, 0, labels, productCategory);
                process(mapList, 1, labels, brandCategory);
                process(mapList, 2, labels, starCategoryEn);
                process(mapList, 3, labels, starCategoryZh);
                process(mapList, 4, labels, appCategory);
                process(mapList, 5, labels, locationCategory);
                process(mapList, 6, labels, musicCategory);
//                process(mapList, 6, labels, movieCategory);

                processWordCount(mapList2, 0, labels, productCategory);
                processWordCount(mapList2, 1, labels, brandCategory);
                processWordCount(mapList2, 2, labels, starCategoryEn);
                processWordCount(mapList2, 3, labels, starCategoryZh);
                processWordCount(mapList2, 4, labels, appCategory);
                processWordCount(mapList2, 5, labels, locationCategory);
                processWordCount(mapList2, 6, labels, musicCategory);
            }
        }

        String[] array = new String[8];
        String[] array2 = new String[8];
        array[0] = key.toString();
        array2[0] = key.toString();
        for(int i = 1; i < array.length; i++) {
            array[i] = JSON.toJSONString(mapList.get(i - 1), SerializerFeature.UseSingleQuotes);
            array2[i] = JSON.toJSONString(mapList2.get(i - 1), SerializerFeature.UseSingleQuotes);
        }
        mos.write(new Text(BuildCsvString.build(array, ',')), NullWritable.get(), "uidCategoryFreq/data");
        mos.write(key, NullWritable.get(), "uid/data");
        mos.write(new Text(BuildCsvString.build(array2, ',')), NullWritable.get(), "uidWordCount/data");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    private void process(List<Map<String, Long>> mapList, int index, List<String> labels, LabelCategory labelCategory) {
        int column = labelCategory.getColumn();
        String labelFreq = labels.get(column);
        Map<String, Long> current = mapList.get(index);
        Map<String, Integer> map = new HashMap<>();
        try {
            map = GsonUtil.getGson().fromJson(labelFreq,
                    new TypeToken<Map<String, Integer>>() {
                    }.getType());
        } catch(Exception e) {
            logger.error("error line:" + labels);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String category = labelCategory.getCategory().get(entry.getKey());
            if (category == null) {
                continue;
            }
            current.putIfAbsent(category, 0L);
            current.put(category, current.get(category) + entry.getValue());
        }
    }

    private void processWordCount(List<Map<String, Long>> mapList, int index, List<String> labels, LabelCategory labelCategory) {
        int column = labelCategory.getColumn();
        String labelFreq = labels.get(column);
        Map<String, Long> current = mapList.get(index);
        Map<String, Integer> map = new HashMap<>();
        try {
            map = GsonUtil.getGson().fromJson(labelFreq,
                new TypeToken<Map<String, Integer>>() {
                }.getType());
        } catch(Exception e) {
            logger.error("error line:" + labels);
        }
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String word = entry.getKey();
            current.merge(word, Long.valueOf(entry.getValue()), Long::sum);
        }
    }

}
