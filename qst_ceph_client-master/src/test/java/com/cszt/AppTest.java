package com.cszt;


import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.IOUtils;
import com.cszt.util.CephException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) throws CephException {
        //配置文件地址，需要和底层ceph公有，需要放在服务器上，默认地址/ect/rgw_java.conf
//        String configPath = "D:\\work\\大数据\\千视通\\rgw\\config\\rgw_java.conf";
//        CephClient.init(configPath);//系统启动时，需要初始化一次
//
//        String fileName = "123456.png";
//        String path = "D:\\work\\大数据\\千视通\\png\\";
////        CephClient.putObject(path, fileName);
//        try {
//            byte[] fileByte = IOUtils.toByteArray(new FileInputStream(new File(path + fileName)));
//            PutObjectResult ret = CephClient.putObject(fileByte, fileName);
//            System.out.println(ret);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//        String objectUrl = CephClient.getObjectUrl(fileName);
//        System.out.println(objectUrl);
//        boolean flag = CephClient.downloadObject("/home/", fileName);
//        System.out.println(flag);
//        boolean delFlag = CephClient.deleteObject(fileName);
//        System.out.println(delFlag);
    }

//    public static void putObject() throws CephException {
//        String fileName = "12345.png";
//        CephClient.putObject("D:\\work\\大数据\\千视通\\png", fileName);
//    }
//
//    public static void getObject() throws CephException {
//        String objectUrl = CephClient.getObjectUrl("12345.png");
//        System.out.println(objectUrl);
//    }
}
