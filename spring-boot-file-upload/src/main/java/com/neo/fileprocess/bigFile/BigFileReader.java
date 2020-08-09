package com.neo.fileprocess.bigFile;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 分片大小=文件总大小/线程数
 * <p>
 * 此方法由于分片部分最大值不能超过Integer.MAX_VALUE
 * 所以超过分片大小>Integer.MAX_VALUE需要调整线程数
 * while(分片大小>Integer.MAX_VALUE){线程++}
 */
public class BigFileReader {
    private int threadPoolSize;
    private Charset charset;
    private int bufferSize;
    private IFileHandle handle;
    private ExecutorService executorService;
    private long fileLength;
    private RandomAccessFile rAccessFile;
    private Set<StartEndPair> startEndPairs;
    private CyclicBarrier cyclicBarrier;
    private AtomicLong counter = new AtomicLong(0);

    public BigFileReader(File file, Charset charset, int bufferSize, int threadPoolSize, IFileHandle handle) {
        this.fileLength = file.length();
        this.handle = handle;
        this.charset = charset;
        this.bufferSize = bufferSize;
        this.threadPoolSize = threadPoolSize;

        if (!file.exists()) {
            throw new IllegalArgumentException("文件不存在！");
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("线程池参数必须为大于0的整数");
        }
        try {
            this.rAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        startEndPairs = new HashSet<StartEndPair>();
    }

    public void start() {
        long everySize = this.fileLength / this.threadPoolSize;
        try {
            calculateStartEnd(0, everySize);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        final long startTime = System.currentTimeMillis();
        cyclicBarrier = new CyclicBarrier(startEndPairs.size(), () -> {
            System.out.println("use time: " + (System.currentTimeMillis() - startTime));
            System.out.println("all line: " + counter.get());
            shutdown();
        });
        for (StartEndPair pair : startEndPairs) {
            System.out.println("分配分片：" + pair);
            this.executorService.execute(new SliceReaderTask(pair));
        }
    }

    private void calculateStartEnd(long start, long size) throws IOException {
        if (start > fileLength - 1) {
            return;
        }
        StartEndPair pair = new StartEndPair();
        pair.start = start;
        long endPosition = start + size - 1;
        if (endPosition >= fileLength - 1) {
            pair.end = fileLength - 1;
            startEndPairs.add(pair);
            return;
        }

        rAccessFile.seek(endPosition);
        byte tmp = (byte) rAccessFile.read();
        while (tmp != '\n' && tmp != '\r') {
            endPosition++;
            if (endPosition >= fileLength - 1) {
                endPosition = fileLength - 1;
                break;
            }
            rAccessFile.seek(endPosition);
            tmp = (byte) rAccessFile.read();
        }
        pair.end = endPosition;
        startEndPairs.add(pair);

        calculateStartEnd(endPosition + 1, size);

    }

    public void shutdown() {
        try {
            this.rAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.executorService.shutdown();
    }

    private void handle(byte[] bytes) throws UnsupportedEncodingException, InterruptedException {
        String line = null;
        if (this.charset == null) {
            line = new String(bytes);
        } else {
            line = new String(bytes, charset);
        }
        if (line != null && !"".equals(line)) {
            this.handle.handle(line);
            counter.incrementAndGet();
        }
    }


    private static class StartEndPair {
        public long start;
        public long end;

        @Override
        public String toString() {
            return "star=" + start + ";end=" + end;
        }
    }

    private class SliceReaderTask implements Runnable {
        private long start;
        private long sliceSize;
        private byte[] readBuff;

        public SliceReaderTask(StartEndPair pair) {
            this.start = pair.start;
            this.sliceSize = pair.end - pair.start + 1;
            this.readBuff = new byte[bufferSize];
        }


        @Override
        public void run() {
            try {
                MappedByteBuffer mapBuffer = rAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, start, this.sliceSize);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (int offset = 0; offset < sliceSize; offset += bufferSize) {
                    int readLength;
                    if (offset + bufferSize <= sliceSize) {
                        readLength = bufferSize;
                    } else {
                        readLength = (int) (sliceSize - offset);
                    }
                    mapBuffer.get(readBuff, 0, readLength);
                    for (int i = 0; i < readLength; i++) {
                        byte tmp = readBuff[i];
                        //碰到换行符
                        if (tmp == '\n' || tmp == '\r') {
                            handle(bos.toByteArray());
                            bos.reset();
                        } else {
                            bos.write(tmp);
                        }
                    }
                }
                if (bos.size() > 0) {
                    handle(bos.toByteArray());
                }
                cyclicBarrier.await();//测试性能用
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public interface IFileHandle {
        void handle(String line) throws InterruptedException;
    }

    /**
     * 获取文件行数
     *
     * @param fliePath
     * @return
     * @throws IOException
     */
    public static long getLineNumber(String fliePath) throws IOException {
        try (FileReader fileReader = new FileReader(fliePath);
             LineNumberReader lineNumberReader = new LineNumberReader(fileReader);) {
            lineNumberReader.skip(Long.MAX_VALUE);
            return lineNumberReader.getLineNumber();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 采用BufferedInputStream方式读取文件行数
     *
     * @param filename
     * @return
     * @throws IOException
     */
    public static int getLineCount(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        byte[] c = new byte[1024];
        int count = 0;
        int readChars = 0;
        while ((readChars = is.read(c)) != -1) {
            for (int i = 0; i < readChars; ++i) {
                if (c[i] == '\n')
                    ++count;
            }
        }
        is.close();
        return count;
    }

    /**
     * 采用BufferedReader方式读取总行数
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    public static long getTotalLines2(String fileName) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String strLine = br.readLine();
        long totalLines = 0;
        while (strLine != null) {
            if (!"".equals(strLine)) {
                totalLines++;
            }
            strLine = br.readLine();
        }
        br.close();
        return totalLines;
    }
}
