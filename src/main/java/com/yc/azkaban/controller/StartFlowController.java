package com.yc.azkaban.controller;

import com.exception.AppException;
import com.yc.azkaban.module.param.ExecuteFlowParam;
import com.yc.azkaban.service.FlowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StartFlowController {
    private final static Logger logger = LoggerFactory.getLogger(StartFlowController.class);

    @Autowired
    FlowService FlowService;

    @GetMapping("/azkaban/flow/executeFlow")
    public String executeFlow(ExecuteFlowParam param) throws AppException {
        return FlowService.executeFlow(param);
    }

    @GetMapping("/azkaban/flow/executeJobAndDownstream")
    public String executeJobAndDownstream(ExecuteFlowParam param) throws AppException {
        return FlowService.executeJobAndDependence(param);
    }

    @GetMapping("/azkaban/flow/executeJob")
    public String executeJob(ExecuteFlowParam param) throws AppException {
        return FlowService.executeJob(param);
    }
}
