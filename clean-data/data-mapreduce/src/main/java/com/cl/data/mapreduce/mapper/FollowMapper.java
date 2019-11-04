package com.cl.data.mapreduce.mapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.cl.data.mapreduce.bean.FollowRelationship;
import com.cl.data.mapreduce.dto.FollowingRelationshipDTO;
import com.cl.data.mapreduce.util.UserTypeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * @author yejianyu
 * @date 2019/8/20
 */
public class FollowMapper extends Mapper<Object, Text, Text, FollowRelationship> {

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        URI[] uidUris = Job.getInstance(conf).getCacheFiles();
        for (URI uidUri : uidUris) {
            Path uidPath = new Path(uidUri.getPath());
            String uidFileName = uidPath.getName();
            UserTypeUtils.initSet(uidFileName);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        List<FollowingRelationshipDTO> relationshipList;
        try {
            relationshipList = JSON.parseArray(value.toString(), FollowingRelationshipDTO.class);
        } catch (JSONException e) {
            System.err.println("json解析出错：" + StringUtils.stringifyException(e));
            return;
        }
        for (FollowingRelationshipDTO followingRelationship : relationshipList) {
            String toUid = followingRelationship.getTo();
            if (!UserTypeUtils.isCoreUser(toUid)) {
                continue;
            }

            String fromUid = followingRelationship.getFrom();
            if (!UserTypeUtils.isCoreUser(fromUid)) {
                continue;
            }
            String fromUserType = UserTypeUtils.getUserType(fromUid);
            String toUserType = UserTypeUtils.getUserType(toUid);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(fromUserType)
                    && org.apache.commons.lang3.StringUtils.isNotBlank(toUserType)) {
                String relationship = fromUserType + "_following_" + toUserType;
                context.write(new Text(relationship), new FollowRelationship(new Text(fromUid), new Text(toUid)));
            }
        }
    }
}