package com.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPObject;
import com.constant.AzkabanConstant;
import com.exception.AppException;
import com.yc.azkaban.module.response.FlowNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.*;

public class AzkabanUtils {
    private final static Logger logger = LoggerFactory.getLogger(AzkabanUtils.class);
    private String sessionId;

    public AzkabanUtils() {
        try {
            this.getSessionId();
        }catch (AppException e){
            e.printStackTrace();
            logger.error("AZ初始化异常，获取sessionID失败");
        }
    }

    private static class AzkabanUtilsHoler{
        private static AzkabanUtils INSTANCE = new AzkabanUtils();
    }

    public static AzkabanUtils getInstance(){
        return AzkabanUtilsHoler.INSTANCE;
    }

    private String getSessionId() throws AppException {
        if(!StringUtils.isBlank(this.sessionId)){
            return this.sessionId;
        }
        logger.info("开始登录，获取session.id");
        synchronized (this){
            HashMap<String,String> parmMap=new HashMap<String,String> ();
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_ACTION,AzkabanConstant.AZ_ACTION_LOGIN);
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_USERNAME,AzkabanConstant.AZ_USER_NAME);
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_PASSWORD,AzkabanConstant.AZ_PASSWORD);
            try{
                String response=HttpClientUtil.post(AzkabanConstant.AZ_DOMAIN,parmMap);
                if(!checkResponseString(response)){
                    logger.error("获取session.id失败，返回结果验证异常！");
                    throw new AppException("获取session.id失败，返回结果验证异常！");
                }
                JSONObject loginJson=JSONObject.parseObject(response);
                this.sessionId=loginJson.getString(AzkabanConstant.AZ_PARAM_NAME_SESSION_ID);
                if(sessionId==null){
                    logger.error("获取session.id失败，返回sessionID为空");
                    throw new AppException("获取session.id失败，返回sessionID为空");
                }
                logger.info("获取session.id成功！");
            }catch (Exception e){
                e.printStackTrace();
                logger.error("获取session.id，异常！");
                throw new AppException("获取session.id，异常！");
            }
        }
        return this.sessionId;
    }

    public void clearSessionId(){
        this.sessionId="";
    }

    public boolean checkSession() throws AppException {
        HashMap<String,String> parmMap=new HashMap<String,String> ();
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_SESSION_ID,this.sessionId);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_AJAX,AzkabanConstant.AZ_ACTION_FETCHPROJECTFLOWS);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_PROJECT,"yiche_log_hr");
        logger.info("开始验证登录是否有效！");
        try{
            if(StringUtils.isBlank(this.sessionId)){
                logger.info("sessionid为空");
                return false;
            }
            String response=HttpClientUtil.get(AzkabanConstant.AZ_DOMAIN+"/"+
                    AzkabanConstant.AZ_REPUEST_API_MANAGER,parmMap);
            if(!checkResponseString(response)){
                logger.info("验证登录返回JSON无效！");
                return false;
            }
            if(!responseIsAuth(response)){
                logger.info("当前sessionid失效开始重新登录！");
                return false;
            }
            logger.info("当前sessionid有效！");
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.error("验证登录失败！");
            throw new AppException("验证登录失败！");
        }
    }

    public String login() throws AppException {
        this.clearSessionId();
        return this.getSessionId();
    }

    public String executeFlow(String project, String flow, String[] disabled,
                              Map<String,String> flowOverride,String concurrentOption) throws AppException {
        String execid="-1";
        HashMap<String,String> parmMap=new HashMap<String,String> ();
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_AJAX,AzkabanConstant.AZ_ACTION_EXECUTEFLOW);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_PROJECT,project);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_FLOW,flow);
        if(disabled!=null){
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_DISABLED, com.utils.StringUtils.arrayToJSONString(disabled));
        }
        if(concurrentOption!=null){
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_CONCURRENTOPTION,
                    AzkabanConstant.AZ_PARAM_VALUE_CONCURRENTOPTION_IGNORE);
        }
        for (Entry<String,String> kv:flowOverride.entrySet()) {
            String orrivedName=AzkabanConstant.
                    AZ_PARAM_NAME_FLOWOVERRIDE.replace(
                            AzkabanConstant.AZ_PARAM_NAME_FLAG,kv.getKey());
            parmMap.put(orrivedName,kv.getValue());
        }
        try{
            if(!this.checkSession()){
                this.login();
            }
            parmMap.put(AzkabanConstant.AZ_PARAM_NAME_SESSION_ID,this.sessionId);
            loggerInfo(project,flow,"开始提交az任务");
            String response=HttpClientUtil.get(
                    AzkabanConstant.AZ_DOMAIN+"/"+AzkabanConstant.AZ_REPUEST_API_EXECUTOR,parmMap);
            if(!this.checkResponseString(response)){
                loggerError(project,flow,"返回信息检查异常！");
                return execid;
            }
            JSONObject excJson=JSONObject.parseObject(response);

            String reExecid=excJson.getString("execid");
            if(!StringUtils.isBlank(reExecid)){
                execid=reExecid;
            }
            loggerError(project,flow,"提交az任务成功!"+response);
        }catch (Exception e){
            e.printStackTrace();
            loggerError(project,flow,"提交az任务失败");
            throw new AppException("提交az任务失败");
        }
        return execid;
    }


    private boolean checkResponseString(String response){
        if(StringUtils.isBlank(response)){
            logger.error("返回信息为空！");
            return false;
        }
        if(!JSONObject.isValid(response)){
            logger.error("返回信息判断JSON格式异常！");
            return false;
        }
        JSONObject responseJson=JSONObject.parseObject(response);
        String error=responseJson.getString("error");
        if(!StringUtils.isBlank(error)){
            logger.error("请求失败返回错误信息："+error);
        }
        return true;
    }

    private boolean responseIsAuth(String response){
        JSONObject responseJson=JSONObject.parseObject(response);
        String error=responseJson.getString("error");
        if("session".equals(error)){
            logger.error("会话失效，重新登录！");
            return false;
        }
        return true;
    }

    public void loggerError(String project, String flow,String msg){
        logger.error("project:"+project+" flow:"+flow+"msg"+msg);
    }

    public void loggerInfo(String project, String flow,String msg){
        logger.error("project:"+project+" flow:"+flow+"msg"+msg);
    }

    public List<FlowNode> getJobListByFlow(String projectName,String flowName) throws AppException {
        if(!this.checkSession()){
            this.login();
        }
        HashMap<String,String> parmMap=new HashMap<String,String> ();
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_SESSION_ID,this.sessionId);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_AJAX,AzkabanConstant.AZ_ACTION_FETCHFLOWGRAPH);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_PROJECT,projectName);
        parmMap.put(AzkabanConstant.AZ_PARAM_NAME_FLOW,flowName);
        List nodeList=new ArrayList<FlowNode>();
        try{
            loggerInfo(projectName,flowName,"查询流下的所有job");
            String response=HttpClientUtil.get(
                    AzkabanConstant.AZ_DOMAIN+"/"+AzkabanConstant.AZ_REPUEST_API_MANAGER,parmMap);
            if(!this.checkResponseString(response)){
                loggerError(projectName,flowName,"返回信息检查异常！");
                return nodeList;
            }
            JSONObject flowGraphJson=JSONObject.parseObject(response);
            JSONArray nodes=flowGraphJson.getJSONArray("nodes");
            for (Object nodeObj: nodes) {
                FlowNode flowNode=JSONObject.parseObject(nodeObj.toString(), FlowNode.class);
                nodeList.add(flowNode);
            }
            loggerError(projectName,flowName,"查询流下的所有job成功!"+response);
        }catch (Exception e){
            e.printStackTrace();
            loggerError(projectName,flowName,"查询流下的所有job失败");
            throw new AppException("查询流下的所有job失败");
        }
        return nodeList;
    }

    public static void main(String[] args) throws AppException {
        AzkabanUtils azkabanUtils= AzkabanUtils.getInstance();
        List<FlowNode> list=azkabanUtils.getJobListByFlow("yiche_pcm_scale_dw_dy","yiche_pcm_scale_dw_dy_finish");
        for (FlowNode node:list) {
            System.out.println(node);
        }
    }
}
