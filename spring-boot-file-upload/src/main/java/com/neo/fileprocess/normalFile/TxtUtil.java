package com.neo.fileprocess.normalFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

/**
 * txt读取工具
 * @author sz
 *
 * create time：2013-1-31
 */
@Slf4j
public abstract class TxtUtil {

    private File file;

    private String encoding = "GBK"; // 字符编码(可解决中文乱码问题 )

    protected int rowTotal = 0;// 记录总行数

    protected boolean flag;

    private int mode = 0; //是否开启读剩余数据的模式，默认：0 不开启 ， 1开启

    public TxtUtil(File file) {
        this.file = file;
    }

    public TxtUtil(File file, int mode) {

        this.file = file;
        this.mode = mode;
    }



    /**
     * 执行 读取txt文本，返回读取行数
     * @return
     * @throws Exception
     */
    public int executeRead() throws Exception {

        BufferedReader bufferedReader = null;

        try {

            if (file.isFile() && file.exists()) {

                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);

                bufferedReader = new BufferedReader(read);

                String lineTXT = null;

                while ((lineTXT = bufferedReader.readLine()) != null) {

                    if (!StringUtils.isEmpty(lineTXT)) {// 不等于空才运行
                        concreteProcess(lineTXT);// 使用抽象方法处理每行的字符串
                    }
                }

                if(mode == 1){//如果读剩余模式开启才执行

                    flag = true;//证明已经读完了

                    concreteProcess(lineTXT);//再执行一次，把剩余的行数处理
                }

            } else {

                log.error("找不到指定的文件！");

            }

        } catch (IOException e) {

            log.error("读取文件内容操作出错");
            e.printStackTrace();
            throw e;//再抛出去

        } finally {

            if (bufferedReader != null)
                bufferedReader.close();
        }

        return this.rowTotal;//返回读取的行数

    }

    /**
     * 实现该类时，把这个方法的交给匿名类实现
     *
     * @param str
     */
    public abstract void concreteProcess(String str) throws Exception;

}
