package com.cl.data.mapreduce.mapper.pesg;

import com.cl.data.mapreduce.constant.WordSegmentationConstants;

import java.util.HashSet;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/9/28
 */
public class PesgTypeResultThreeMapper extends BasePesgTypeResultMapper {

    private static final Set<String> UID_SET = new HashSet<>();

    @Override
    protected Set<String> getUidSet() {
        return UID_SET;
    }

    @Override
    protected String getType(){
        return WordSegmentationConstants.TYPE_RESULT_3;
    }

}
