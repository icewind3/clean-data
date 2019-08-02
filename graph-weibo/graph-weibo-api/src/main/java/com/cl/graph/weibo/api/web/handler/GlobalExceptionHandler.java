package com.cl.graph.weibo.api.web.handler;

import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.core.web.ResponseResult;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author yejianyu
 * @date 2019/7/5
 */
@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(value = ServiceException.class)
    @ResponseBody
    public ResponseResult handleServiceException(Exception e) {
        e.printStackTrace();
        return ResponseResult.error(e.getMessage());
    }
}
