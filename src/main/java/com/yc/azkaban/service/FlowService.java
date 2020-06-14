package com.yc.azkaban.service;

import com.constant.AzkabanConstant;
import com.exception.AppException;
import com.utils.AzkabanUtils;
import com.yc.azkaban.module.param.ExecuteFlowParam;
import com.yc.azkaban.module.response.FlowNode;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class FlowService {
    private final static Logger logger = LoggerFactory.getLogger(FlowService.class);

    AzkabanUtils azkabanUtils=AzkabanUtils.getInstance();

    @Autowired
    private Environment environment;

    public String executeFlow(ExecuteFlowParam param) throws AppException {
        String[] disArry=param.getDis();
        String concurrentOption=null;

        if(!"1".equals(param.getIsAll())){
            if(disArry==null&&disArry.length<=0){
                String disable=environment.getProperty("azkaban."+param.getProject()+"."+param.getFlow()+".disabled");
                if(disable!=null){
                    disArry=disable.split(",");
                }
            }
        }
        String isConcurrent=param.getConcurrent();
        if("1".equals(isConcurrent)){
            concurrentOption= AzkabanConstant.AZ_PARAM_VALUE_CONCURRENTOPTION_IGNORE;
        }
        Map<String,String> varMap=new HashMap<String,String>();
        varMap.put("dt",param.getDt());
        varMap.put("hr",param.getHour());
        String execId=azkabanUtils.executeFlow(param.getProject()
                ,param.getFlow()
                ,disArry,
                varMap,concurrentOption);
        return execId;
    }

    public String executeJobAndDependence(ExecuteFlowParam param) throws AppException {
        String project=param.getProject();
        String flow=param.getFlow();
        String job=param.getJob();

        List<FlowNode> nodes=azkabanUtils.getJobListByFlow(project,flow);
        if(nodes.size()<=0){
            return "无法查到"+project+"中的"+flow+"的所属job列表";
        }
        List<String> allList=this.toStringList(nodes);

        String[] disArry={};
        List<String> depList=this.getDependence(job,nodes);

        allList.removeAll(depList);
        disArry=allList.toArray(new String[]{});
        logger.info("执行"+project+"->"+flow+"->"+job+"，"+"当前流禁用job："+ArrayUtils.toString(disArry));

        param.setDis(disArry);
        param.setIsAll("0");
        return this.executeFlow(param);
    }

    public List<String> getDependence(String job,List<FlowNode> nodes){
        List<String> dependenceNode=new ArrayList<String>();
        List<String>  tmp=new ArrayList();
        tmp.add(job);
        while (tmp!=null&&tmp.size()>0){
            List<String> currDep=new ArrayList();
            for (String depJob:tmp) {
                dependenceNode.add(depJob);
                for (FlowNode node:nodes) {
                    String nodeId=node.getId();
                    String[] in=node.getIn();
                    if(in!=null&&in.length>0){
                        for (String jobId: in) {
                            if(jobId.equals(depJob)){
                                currDep.add(nodeId);
                                break;
                            }
                        }
                    }

                }
            }
            if(currDep.size()>=0){
                tmp=currDep;
            }
        }
        return dependenceNode;
    }


    public List<String> toStringList(List<FlowNode> nodes){
        if(nodes.size()<=0){
            return null;
        }
        List<String> allList=new ArrayList<String>();
        for (FlowNode node: nodes) {
            allList.add(node.getId());
        }
        return allList;
    }

    public List<String> getAllNodes(String project,String flow) throws AppException {
        List<FlowNode> nodes=azkabanUtils.getJobListByFlow(project,flow);
        List<String> allList=this.toStringList(nodes);
        return allList;
    }

    public String executeJob(ExecuteFlowParam param) throws AppException {
        String project=param.getProject();
        String flow=param.getFlow();
        String job=param.getJob();

        String[] disArry={};
        List<String> allList=this.getAllNodes(project,flow);
        if(allList.size()<=0){
            return "无法查到"+project+"中的"+flow+"的所属job列表";
        }

        allList.remove(job);
        disArry=allList.toArray(new String[]{});
        logger.info("执行"+project+"->"+flow+"->"+job+"，"+"当前流禁用job："+ArrayUtils.toString(disArry));

        param.setDis(disArry);
        param.setIsAll("0");
        return this.executeFlow(param);
    }


    public static void main(String[] args) throws AppException {
        FlowService flowService=new FlowService();
        ExecuteFlowParam param=new ExecuteFlowParam();
        param.setProject("yiche_pcm_scale_dw_dy");
        param.setFlow("yiche_pcm_scale_dw_dy_finish");
        param.setJob("yiche_pcm.yiche_pcm_ref_serial_pv_fact_hive2hive");
        param.setConcurrent("1");
        flowService.executeJob(param);
    }

}
