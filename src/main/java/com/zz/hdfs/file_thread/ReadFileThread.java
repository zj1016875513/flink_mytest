package com.zz.hdfs.file_thread;

/**
 * Created with IntelliJ IDEA.
 * User: okey
 * Date: 14-4-2
 * Time: 下午4:50
 * To change this template use File | Settings | File Templates.
 */
public class ReadFileThread extends Thread {
    private ReaderFileListener processPoiDataListeners;
    private String filePath;
    private long start;
    private long end;
    public ReadFileThread(ReaderFileListener processPoiDataListeners,long start,long end,String file) {
        this.setName(this.getName()+"-ReadFileThread");
        this.start = start;
        this.end = end;
        this.filePath = file;
        this.processPoiDataListeners = processPoiDataListeners;
    }
    @Override
    public void run() {
        ReadFile readFile = new ReadFile();
        readFile.setReaderListener(processPoiDataListeners);
        readFile.setEncode(processPoiDataListeners.getEncode());
// readFile.addObserver();
        try {
            readFile.readFileByLine(filePath, start, end + 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
