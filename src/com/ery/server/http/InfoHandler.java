package com.ery.server.http;

import org.mortbay.jetty.Server;

/**
 * Copyrights @ 2013,Tianyuan DIC Information Co.,Ltd. All rights reserved.
 *
 * @author wangcs
 * @description
 * @date 14-1-15
 * -
 * @modify
 * @modifyDate -
 */
public abstract class InfoHandler implements org.mortbay.jetty.Handler{

    @Override
    public Server getServer() {
        return null;
    }

    @Override
    public void setServer(Server server) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isStarting() {
        return false;
    }

    @Override
    public boolean isStopping() {
        return false;
    }

    @Override
    public boolean isStopped() {
        return false;
    }

    @Override
    public boolean isFailed() {
        return false;
    }

    @Override
    public void addLifeCycleListener(Listener listener) {
    }

    @Override
    public void removeLifeCycleListener(Listener listener) {
    }
}
