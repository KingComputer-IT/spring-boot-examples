package com.neo.service.impl;

import com.neo.fileprocess.bigFile.BigFileReader;
import com.neo.fileprocess.normalFile.TxtUtil;
import com.neo.model.dto.RequestDTO;
import com.neo.model.entity.HandUp;
import com.neo.service.FileProcessService;
import com.neo.utils.SnowflakeIdWorker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * 方法类地址：https://blog.csdn.net/u013905744/article/details/74990072
 */
@Slf4j
@Service
public class FileProcessServiceImpl implements FileProcessService {

    private static final int BATCH_SUM = 5000;


    @Override
    public String ImportTxtForBigFile(RequestDTO requestDTO) throws ExecutionException, InterruptedException {
        final long start = System.currentTimeMillis();
        FutureTask<Long> futureTask = new FutureTask<>(new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                final long totalLines = BigFileReader.getTotalLines2(requestDTO.getFileName());
                return totalLines;
            }
        });
        new Thread(futureTask).start();
        File file = new File(requestDTO.getFileName());
        Integer threadPoolSize = requestDTO.getThreadPoolSize();
        //设置一个有界阻塞队列
        ArrayBlockingQueue<String> arrayQueue = new ArrayBlockingQueue<>(10000);
        new BigFileReader(file, StandardCharsets.UTF_8, 1024 * 1024, threadPoolSize, line -> {
            //处理内部逻辑
            if (!StringUtils.isEmpty(line)) {
                arrayQueue.put(line);
            }
        }).start();
        String groupCode = "YY" + SnowflakeIdWorker.generateId();
        final int threadNum = requestDTO.getThreadNum();
        final Long tatalLine = futureTask.get();
        LongAdder count = new LongAdder();
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            executorService.execute((Runnable) new HandUpTaskExecutor(arrayQueue, countDownLatch, count, tatalLine, groupCode));
        }
        countDownLatch.await();
        executorService.shutdownNow();
        final long end = System.currentTimeMillis();
        return "test成功导入数据{" + count.longValue() + "}条！总计耗时：" + (end - start);
    }


    class HandUpTaskExecutor implements Callable {
        private ArrayBlockingQueue<String> arrayQueue;
        private CountDownLatch countDownLatch;
        private LongAdder count;
        private Long tatalLine;
        private String groupCode;

        public HandUpTaskExecutor(ArrayBlockingQueue<String> arrayQueue, CountDownLatch countDownLatch, LongAdder count, Long tatalLine, String groupCode) {
            this.arrayQueue = arrayQueue;
            this.countDownLatch = countDownLatch;
            this.count = count;
            this.tatalLine = tatalLine;
            this.groupCode = groupCode;
        }

        @Override
        public Object call() throws Exception {
            HandUp handUp = null;
            ArrayList<HandUp> arrayList = new ArrayList();
            while (count.longValue() < tatalLine) {
                log.info("陷入循环无法自拔。。。。。");
                String take = arrayQueue.poll(1, TimeUnit.SECONDS);
                if (!StringUtils.isEmpty(take)) {
                    handUp = new HandUp();
                    handUp.setGroupCode(groupCode);
                    handUp.setLine(take);
                    arrayList.add(handUp);
                    //文本总数增加
                    count.increment();
                }
                if (arrayList.size() % BATCH_SUM == 0) {
                    //批量添加到数据库
                    insetBetch(arrayList);
                    arrayList.clear();
                }

                //所以行数全部解析结束
                if (count.longValue() == tatalLine) {
                    //尾数的行
                    if (!ObjectUtils.isEmpty(arrayList)) {
                        insetBetch(arrayList);
                        arrayList.clear();
                    }
                    break;
                }
            }
            countDownLatch.countDown();
            return null;
        }
    }

    @Override
    public int ImportTxtForNormal(File txtFile) throws SQLException {

        String sql = "insert into actPlan_list_manage(mobile) values(?)";

        final Connection conn = DriverManager.getConnection("uri", "username", "password");


        final PreparedStatement pstmt = conn.prepareStatement(sql);

        conn.setAutoCommit(false);

        //实现TxtUtil的concreteProcess方法，这个是匿名类，继承了TxtUtil
        TxtUtil txtUtil = new TxtUtil(txtFile, 1) {

            private List<String> mobileList = new ArrayList<String>();// 一个数组存放txt读取的电话

            public void concreteProcess(String str) throws Exception {

                if (!StringUtils.isEmpty(str)) {

                    this.rowTotal++;// 文本总数增加

                    mobileList.add(str);// 增加一个电话
                }

                try {

                    if (rowTotal % 500 == 0) {// 每500个一次

                        for (String mobile : mobileList) {
                            pstmt.setString(2, mobile);
                            pstmt.addBatch();// 用PreparedStatement的批量处理
                        }

                        pstmt.executeBatch();// 执行批量插入

                        mobileList.clear();
                    }

                    if (flag) {// 如果大于等于总数了，把剩余的数据都插入数据库

                        for (String mobile : mobileList) {
                            pstmt.setString(2, mobile);
                            pstmt.addBatch();// 用PreparedStatement的批量处理
                        }

                        pstmt.executeBatch();// 执行批量插入

                        mobileList.clear();
                    }

                } catch (Exception e) {

                    e.printStackTrace();
                    throw e;

                }

            }

        };// new TxtUtil(txtFile) end

        int rowTotal = 0;

        try {

            rowTotal = txtUtil.executeRead();//执行读取

            conn.commit();

        } catch (Exception e) {

            e.printStackTrace();

        } finally {
            conn.close();
        }

        return rowTotal;

    }

    @Override
    public int insetBetch(List<HandUp> list) {
        return 0;
    }
}
