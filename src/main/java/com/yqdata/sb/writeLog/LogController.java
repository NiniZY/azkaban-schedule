package com.yqdata.sb.writeLog;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LogController {
    private final static Logger logger = LoggerFactory.getLogger(LogController .class);

    @GetMapping("/receiveLog")
    public String hello(@RequestParam(value = "remoteLog", defaultValue = "") String remoteLog) {

        logger.error(remoteLog);
        return String.format("OK");
    }
}
