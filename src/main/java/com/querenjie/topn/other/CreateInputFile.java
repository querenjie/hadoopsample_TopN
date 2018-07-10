package com.querenjie.topn.other;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateInputFile {
    public static String inputFile = "D:\\test\\hadoop_data\\topn_data\\in\\inputFile1.txt";
    public static String[] phoneNumbers = null;
    public static int totalPhoneNumberCount = 20;
    static {
        phoneNumbers = new String[totalPhoneNumberCount];
        for (int i =0; i < totalPhoneNumberCount; i++) {
            phoneNumbers[i] = String.valueOf(111000000 + i);
        }
    }

    /**
     * 获取一个电话号码
     * @return
     */
    public static String getPhoneNumber() {
        Random random = new Random();
        int index = random.nextInt(totalPhoneNumberCount);
        return phoneNumbers[index];
    }

    /**
     * 获取上传流量，是一个1-100之间的数字
     * @return
     */
    public static long getUpFlow() {
        Random random = new Random();
        return random.nextInt(100) + 1;
    }

    /**
     * 获取下载流量，是一个1-100之间的数字
     * @return
     */
    public static long getDownFlow() {
        Random random = new Random();
        return random.nextInt(100) + 1;
    }

    /**
     * 产生一行文本数据{电话号码\t上传流量\t下载流量\n}
     * @return
     */
    public static String createLineContent() {
        String lineContent = getPhoneNumber() + "\t" + getUpFlow() + "\t" + getDownFlow() + "\n";
        return lineContent;
    }

    /**
     * 生成文件
     */
    public static void writeFile() {
        String lineContent = "";
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(inputFile));
            for (long i = 0L; i < 50; i++) {
                lineContent = createLineContent();
                bw.write(lineContent);
                bw.flush();
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("begin to create file.......");
        writeFile();
        System.out.println("file created.");
    }
}
