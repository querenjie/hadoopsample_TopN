package com.querenjie.topn.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsUtil {
    /**
     * 创建FileSystem对象
     * @param hdfsPath
     * @param user          跟文件读写权限有关
     * @return              返回FileSystem对象
     */
    public static FileSystem createFileSystem(String hdfsPath, String user) throws Exception {
        Configuration configuration = new Configuration();
        URI hdfsUri = null;
        try {
            hdfsUri = new URI(hdfsPath);
        } catch (URISyntaxException e) {
            System.err.println("连接hadoop hdfs失败。");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        FileSystem fileSystem = null;
        try {
            if (hdfsUri != null && StringUtils.isNotBlank(user)) {
                    fileSystem = FileSystem.get(hdfsUri, configuration, user);
            } else if (hdfsPath != null && StringUtils.isBlank(user)) {
                fileSystem = FileSystem.get(hdfsUri, configuration);
            }
        } catch (IOException e) {
            System.err.println("创建FileSystem对象时报错。");
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            System.err.println("创建FileSystem对象时报错。");
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return fileSystem;
    }

    /**
     * 从HDFS中下载文件到本地
     * @param fileSystem
     * @param filePathInHDFS
     * @param localDir
     * @param localFileName
     *
     * 调用示例：
     *         System.out.println("开始运行。。。。。。");
     *         String filePathInHDFS = "/test/topn/in/inputFile1.txt";
     *         String localDir = "d:/temp/temp";
     *         String localFileName = "aaa.txt";
     *         FileSystem fileSystem = createFileSystem("hdfs://192.168.1.20:9000", null);
     *         downloadFileFromHDFS(fileSystem, filePathInHDFS, localDir, localFileName);
     *         fileSystem.close();
     *         System.out.println("下载完成。");
     */
    public static void downloadFileFromHDFS(FileSystem fileSystem, String filePathInHDFS, String localDir, String localFileName) {
        File localDirFile = new File(localDir);
        try {
            FileUtils.forceMkdir(localDirFile);
        } catch (IOException e) {
            System.err.println("创建本地目录失败。进程终止。");
            e.printStackTrace();
            return;
        }
        InputStream is = null;
        OutputStream os = null;
        try {
            is = fileSystem.open(new Path(filePathInHDFS));
            os = new FileOutputStream(localDir + "/" + localFileName);
            IOUtils.copyBytes(is, os, 4096, true);
        } catch (IOException e) {
            System.err.println("文件传送过程中出现问题，现在立即终止进程。");
            e.printStackTrace();
            return;
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 将本地的文件复制到HDFS上。
     * 如果HDFS上不存在这个路径则会创建这个路径。
     * 如果HDFS上已经存在这个路径和文件则会覆盖。
     * @param fileSystem
     * @param source
     * @param destination
     *
     * 调用示例：
     *         System.out.println("开始运行。。。。。。");
     *         String source = "d:/test/hadoop_data/topn_data/in/inputFile1.txt";
     *         String destPathInHDFS = "/test//topn/in/";
     *         FileSystem fileSystem = createFileSystem("hdfs://192.168.1.20:9000", "root");
     *         copyFileToHDFS(fileSystem, source, destPathInHDFS);
     *         fileSystem.close();
     *         System.out.println("文件上传完成。");
     */
    public static void copyFileToHDFS(FileSystem fileSystem, String source, String destination) throws Exception {
        // FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
        // 源文件路径
        Path srcPath = new Path(source);
        // 目的路径
        Path dstPath = new Path(destination);
        // 查看目的路径是否存在
        try {
            if (!(fileSystem.exists(dstPath))) {
                // 如果路径不存在，即刻创建
                fileSystem.mkdirs(dstPath);
            }
        } catch (IOException e) {
            System.err.println("文件读写过程中出现错误。");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        // 得到本地文件名称
        String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
        // 将本地文件上传到HDFS
        try {
            fileSystem.copyFromLocalFile(srcPath, dstPath);
            System.out.println("File " + filename + " copied to " + destination);
        } catch (IOException e) {
            System.err.println("本地文件上传到HDFS的过程出现问题。");
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }


    }

    /**
     * 在HDFS上创建目录
     * @param fileSystem
     * @param path
     * @return
     * 调用示例：
     *         System.out.println("开始运行。。。。。。");
     *         String path = "/test/querenjie/ttt";
     *         FileSystem fileSystem = createFileSystem("hdfs://192.168.1.20:9000", null);
     *         boolean isSucess = mkdir(fileSystem, path);
     *         System.out.println("isSuccess:" + isSucess);
     *         if (fileSystem != null) fileSystem.close();
     *         System.out.println("创建目录完成。");
     */
    public static boolean mkdir(FileSystem fileSystem, String path) {
        boolean isSuccess = false;
        Path dirPath = new Path(path);
        //调用mkdir（）创建目录，（可以一次性创建，以及不存在的父目录）
        try {
            isSuccess = fileSystem.mkdirs(dirPath);
        } catch (IOException e) {
            System.err.println("在HDFS中创建目录失败。");
            e.printStackTrace();
            return false;
        }
        return isSuccess;
    }

    /**
     * 从HDFS上删除目录及子目录
     * @param fileSystem
     * @param path
     * @return
     * 调用示例：
     *         System.out.println("开始运行。。。。。。");
     *         String path = "/test/querenjie";
     *         FileSystem fileSystem = createFileSystem("hdfs://192.168.1.20:9000", null);
     *         boolean isSucess = rmdir(fileSystem, path);
     *         System.out.println("isSuccess:" + isSucess);
     *         if (fileSystem != null) fileSystem.close();
     *         System.out.println("删除目录完成。");
     */
    public static boolean rmdir(FileSystem fileSystem, String path) {
        boolean isSuccess = false;
        Path dirPath = new Path(path);
        try {
            isSuccess = fileSystem.deleteOnExit(dirPath);
        } catch (IOException e) {
            System.err.println("在HDFS中删除目录失败。");
            e.printStackTrace();
            return false;
        }
        return isSuccess;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("开始运行。。。。。。");
        String path = "/test/querenjie";
        FileSystem fileSystem = createFileSystem("hdfs://192.168.1.20:9000", null);
        boolean isSucess = rmdir(fileSystem, path);
        System.out.println("isSuccess:" + isSucess);
        if (fileSystem != null) fileSystem.close();
        System.out.println("删除目录完成。");
    }
}
