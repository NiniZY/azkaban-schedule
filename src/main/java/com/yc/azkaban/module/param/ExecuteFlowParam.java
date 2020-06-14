package com.yc.azkaban.module.param;

public class ExecuteFlowParam {
    String project;
    String flow;
    String dt;
    String hour;
    String isAll="0";
    String concurrent="0";
    String job;
    String[] dis;

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public String getIsAll() {
        return isAll;
    }

    public void setIsAll(String isAll) {
        this.isAll = isAll;
    }

    public String getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(String concurrent) {
        this.concurrent = concurrent;
    }

    public String getJob() {
        return job;
    }
    public void setJob(String job) {
        this.job = job;
    }

    public String[] getDis() {
        return dis;
    }

    public void setDis(String[] dis) {
        this.dis = dis;
    }
}
