package com.cl.data.mapreduce;

import com.cl.data.mapreduce.bean.LabelCategory;
import com.cl.data.mapreduce.mapper.CategoryStatisticMapper;
import com.cl.data.mapreduce.reducer.CategoryStatisticReducer;
import com.cl.data.mapreduce.util.GsonUtil;
import com.csvreader.CsvReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CategoryStatistic {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String filepath = args[0];
        String fileOut = args[1];
        JobControl jobControl = new JobControl("分词统计");
        Job job = categoryJob(filepath, fileOut);
        ControlledJob controlledJob = new ControlledJob(job.getConfiguration());
        jobControl.addJob(controlledJob);

        Thread thread = new Thread(jobControl);
        thread.start();
        while (!jobControl.allFinished()) {
            Thread.sleep(5000L);
        }
        jobControl.stop();
//        getTopWord(fileOut, 100);

    }

//    private static void getTopWord(String fileOut, int top) throws IOException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);
//        Path path = new Path("hdfs://clHadoopCluster/" + fileOut + "/uidCategoryFreq/data-r-0000");
//        FSDataInputStream fdis = fs.open(path);
//        try (BufferedReader br = new BufferedReader(new InputStreamReader(fdis, StandardCharsets.UTF_8))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//
//            }
//        }
//    }

    private static Job categoryJob(String filepath, String fileout) throws IOException {
        Configuration configuration = new Configuration();

        configuration.setInt("column.count", 35);
        String productPath = "deploy/product.csv";
        Map<String, String> productCategoryMap = readCategory(productPath);
        LabelCategory productCategory = new LabelCategory();
        productCategory.setColumn(19);
        productCategory.setCategory(productCategoryMap);
        configuration.set("product.category", GsonUtil.getGson().toJson(productCategory));

        String appPath = "deploy/app.csv";
        Map<String, String> appCategoryMap = readCategory(appPath);
        LabelCategory appCategory = new LabelCategory();
        appCategory.setColumn(2);
        appCategory.setCategory(appCategoryMap);
        configuration.set("app.category", GsonUtil.getGson().toJson(appCategory));

        String brandPath = "deploy/brand.csv";
        Map<String, String> brandCategoryMap = readCategory(brandPath);
        LabelCategory brandCategory = new LabelCategory();
        brandCategory.setColumn(5);
        brandCategory.setCategory(brandCategoryMap);
        configuration.set("brand.category", GsonUtil.getGson().toJson(brandCategory));

        String cityPath = "deploy/city.csv";
        Map<String, String> cityCategoryMap = readCategory(cityPath);
        LabelCategory cityCategory = new LabelCategory();
        cityCategory.setColumn(10);
        cityCategory.setCategory(cityCategoryMap);
        configuration.set("city.category", GsonUtil.getGson().toJson(cityCategory));

        String starPath = "deploy/star.csv";
        Map<String, String> starCategoryMap = readCategory(starPath);
        LabelCategory starCategory = new LabelCategory();
        starCategory.setColumn(30);
        starCategory.setCategory(starCategoryMap);
        configuration.set("star.en.category", GsonUtil.getGson().toJson(starCategory));

        LabelCategory starCategory_zh = new LabelCategory();
        starCategory_zh.setColumn(31);
        starCategory_zh.setCategory(starCategoryMap);
        configuration.set("star.zh.category", GsonUtil.getGson().toJson(starCategory_zh));

        String starGenderPath = "deploy/star_gender.csv";
        Map<String, String> starGenderCategoryMap = readCategory(starGenderPath);
        LabelCategory starGenderCategory = new LabelCategory();
        starGenderCategory.setColumn(30);
        starGenderCategory.setCategory(starGenderCategoryMap);
        configuration.set("star.en.gender.category", GsonUtil.getGson().toJson(starGenderCategory));

        LabelCategory starGenderCategory_zh = new LabelCategory();
        starGenderCategory_zh.setColumn(31);
        starGenderCategory_zh.setCategory(starCategoryMap);
        configuration.set("star.zh.gender.category", GsonUtil.getGson().toJson(starGenderCategory_zh));

        String starNationPath = "deploy/star_nation.csv";
        Map<String, String> starNationCategoryMap = readCategory(starNationPath);
        LabelCategory starNationCategory = new LabelCategory();
        starNationCategory.setColumn(30);
        starNationCategory.setCategory(starNationCategoryMap);
        configuration.set("star.en.nation.category", GsonUtil.getGson().toJson(starNationCategory));

        LabelCategory starNationCategory_zh = new LabelCategory();
        starNationCategory_zh.setColumn(31);
        starNationCategory_zh.setCategory(starNationCategoryMap);
        configuration.set("star.zh.gender.category", GsonUtil.getGson().toJson(starNationCategory_zh));

        String musicPath = "deploy/music.csv";
        Map<String, String> musicCategoryMap = readCategory(musicPath);
        LabelCategory musicCategory = new LabelCategory();
        musicCategory.setColumn(18);
        musicCategory.setCategory(musicCategoryMap);
        configuration.set("music.category", GsonUtil.getGson().toJson(musicCategory));

        Job job = Job.getInstance(configuration, "Statistic Category");
        job.setJarByClass(CategoryStatistic.class);
        job.setMapperClass(CategoryStatisticMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(CategoryStatisticReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(10);
        FileInputFormat.addInputPath(job, new Path(filepath));
        FileOutputFormat.setOutputPath(job, new Path(fileout));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
       return job;
    }

    public static Map<String, String> readCategory(String path) {
        CsvReader reader = null;
        Map<String, String> map = new HashMap<>();
        try {
            reader = new CsvReader(path, ',', Charset.forName("utf-8"));
            while (reader.readRecord()) {
                map.put(reader.get(0), reader.get(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
        }

        return map;
    }
}
