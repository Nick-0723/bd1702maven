package com.briup.client;

import com.briup.util.BIDR;
import com.briup.util.BackUP;
import com.briup.util.Configuration;
import com.briup.util.Logger;
import com.briup.woss.ConfigurationAWare;
import com.briup.woss.client.Client;
import com.briup.woss.client.Gather;

import java.io.*;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;

/**
 * 客户端模块
 * @author Nick
 */
public class WossClient implements Client,ConfigurationAWare {
    private String host;
    private int port;
    private String tempFile;

    private Logger logger;
    private Gather gather;
    private BackUP backUP;
    private Collection<BIDR> bidrs;

    public Collection<BIDR> getBidrs() {
        return bidrs;
    }

    /**
     * @param collection BIDR类对象集合
     * @throws Exception
     */
    @Override
    public void send(Collection<BIDR> collection) throws Exception {
        Socket socket = new Socket(host,port);
        OutputStream out = socket.getOutputStream();
        bidrs = gather.gather();
        //备份
        logger.info("客服端的bidrs开始备份");
        backUP.store(tempFile,bidrs,false);

        logger.info("开始往服务器端传输数据,数据大小:"+bidrs.size());
        ObjectOutputStream outputStream = new ObjectOutputStream(out);
        try {
            outputStream.writeObject(bidrs);
            outputStream.flush();
        } catch (IOException e) {
            logger.error("传输出现错误");
            e.printStackTrace();
        } finally {
            outputStream.close();
            out.close();
        }

        logger.info("传输完成");
    }

    /**
     * 初始化方法
     * @param properties
     */
    @Override
    public void init(Properties properties) {
        host = properties.getProperty("host");
        tempFile = properties.getProperty("tempFile");
        port = Integer.parseInt(properties.getProperty("port"));
    }

    /**
     * 获取配置模块方法
     * @param configuration
     */
    @Override
    public void setConfiguration(Configuration configuration) {
        try {
            gather = configuration.getGather();
            logger = configuration.getLogger();
            backUP = configuration.getBackup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
