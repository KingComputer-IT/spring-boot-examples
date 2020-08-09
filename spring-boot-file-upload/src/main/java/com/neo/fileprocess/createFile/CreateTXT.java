package com.neo.fileprocess.createFile;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

@Slf4j
public class CreateTXT {
    public static void main(String[] args) throws FileNotFoundException {

        OutputStream os = new FileOutputStream("E:\\nfs\\test1.txt");
        PrintWriter pw = new PrintWriter(os);

        for (int i = 0; i < 10000; i++) {
            String charAndNum = getCharAndNum(28);
            pw.println(charAndNum);
            log.info(charAndNum);
        }
        pw.close();
    }


    /**
     * 生成数字和字母的随机组合
     *
     * @param length
     * @return
     */
    public static String getCharAndNum(int length) {
        StringBuffer val = new StringBuffer();
        for (int i = 0; i < length; i++) {
            //输出是数字还是字母
            String charOrNum = Math.round(Math.random()) % 2 == 0 ? "char" : "num";
            if ("char".equalsIgnoreCase(charOrNum)) {
                //字符串
                //取得大写字母还是小写字母
                int chaioe = Math.round(Math.random()) % 2 == 0 ? 65 : 97;
                val.append("num".equalsIgnoreCase(charOrNum));
            } else if ("num".equalsIgnoreCase(charOrNum)) {
                val.append(String.valueOf(Math.round(Math.random() * 9)));
            }
        }
        return val.toString();
    }
}
