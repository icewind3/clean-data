package com.cl.graph.weibo.core.linux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.springframework.beans.BeanUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * @author zpx
 * Build File @date: 2019/2/28 14:39
 * @description
 * @version 1.0
 */
@SuppressWarnings("all")
public class ResultApi<T> {

    /**
     * 定义jackson对象
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * 定义status值类型
     */
    public static final Integer SUCCESSFUL_STATUS = 200;

    private Integer status;

    private String msg;

    private T data;

    private boolean rel;

    public static<T> ResultApi<T> build(Integer status, String msg, T data) {
        return new ResultApi<>(status, msg, data);
    }

    public static<T> ResultApi<T> build(Integer status, T data) {
        return new ResultApi<>(status, data);
    }

    public static<T> ResultApi<T> ok(T data) {
        return new ResultApi<>(data);
    }

    public static<T> ResultApi<List<T>> okAndCopyProperties(List<?> data, Class<T> target) {
        List<T> targetList = new LinkedList<>();
        for (Object source : data) {
            T targetItem = null;
            try {
                targetItem = target.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            BeanUtils.copyProperties(source, Objects.requireNonNull(targetItem));
            targetList.add(targetItem);
        }
        return ResultApi.ok(targetList);
    }

    public static<T> ResultApi<T> okAndCopyProperties(Object data, Class<T> target) {
        T targetObj = null;
        try {
            targetObj = target.newInstance();
            BeanUtils.copyProperties(data, targetObj);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return ResultApi.ok(targetObj);
    }

    public static ResultApi ok() {
        return new ResultApi<>(null);
    }

    public static ResultApi rel(boolean rel) {
        return new ResultApi(rel);
    }

    public static<T> ResultApi<PageData<T>> page(long total, List<T> data) {
        return ResultApi.ok(new PageData<>(total, data));
    }

    public static<T> ResultApi<T> rel(boolean rel, T data) {
        return new ResultApi<>(rel, data);
    }

    public static<T> ResultApi<T> rel(boolean rel, Integer status) {
        return new ResultApi<>(rel, status);
    }

    public ResultApi() {
    }

    public static ResultApi build(Integer status, String msg) {
        return new ResultApi<>(status, msg, null);
    }

    private ResultApi(Integer status, String msg, T data) {
        this.status = status;
        this.msg = msg;
        this.data = data;
    }

    private ResultApi(Integer status, T data) {
        this.status = status;
        this.data = data;
    }

    private ResultApi(boolean rel) {
        this.rel = rel;
    }

    private ResultApi(boolean rel, T data) {
        this.status = rel ? 200 : 500;
        this.rel = rel;
        this.data = data;
    }

    private ResultApi(boolean rel, Integer status) {
        this.rel = rel;
        this.status = status;
    }

    private ResultApi(T data) {
        this.status = ResultApi.SUCCESSFUL_STATUS;
        this.msg = "OK";
        this.data = data;
        this.rel = true;
    }

//    public Boolean isSuccess() {
//        return this.status == 200;
//    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    /**
     * 将json结果集转化为TaotaoResult对象
     *
     * @param jsonData json数据
     * @param clazz TaotaoResult中的object类型
     * @return
     */
    public static ResultApi formatToPojo(String jsonData, Class<?> clazz) {
        try {
            if (clazz == null) {
                return MAPPER.readValue(jsonData, ResultApi.class);
            }
            JsonNode jsonNode = MAPPER.readTree(jsonData);
            JsonNode data = jsonNode.get("data");
            Object obj = null;
            if (clazz != null) {
                if (data.isObject()) {
                    obj = MAPPER.readValue(data.traverse(), clazz);
                } else if (data.isTextual()) {
                    obj = MAPPER.readValue(data.asText(), clazz);
                }
            }
            return build(jsonNode.get("status").intValue(), jsonNode.get("msg").asText(), obj);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 没有object对象的转化
     *
     * @param json
     * @return
     */
    public static ResultApi format(String json) {
        try {
            return MAPPER.readValue(json, ResultApi.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Object是集合转化
     *
     * @param jsonData json数据
     * @param clazz 集合中的类型
     * @return
     */
    public static ResultApi formatToList(String jsonData, Class<?> clazz) {
        try {
            JsonNode jsonNode = MAPPER.readTree(jsonData);
            JsonNode data = jsonNode.get("data");
            Object obj = null;
            if (data.isArray() && data.size() > 0) {
                obj = MAPPER.readValue(data.traverse(),
                        MAPPER.getTypeFactory().constructCollectionType(List.class, clazz));
            }
            return build(jsonNode.get("status").intValue(), jsonNode.get("msg").asText(), obj);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean isRel() {
        return rel;
    }

    public void setRel(boolean rel) {
        this.rel = rel;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Accessors(chain = true)
    public static class PageData<Entity> {
        private long total;
        private List<Entity> rows;
    }
}
