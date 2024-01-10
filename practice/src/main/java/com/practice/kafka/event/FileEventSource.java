package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    public static final Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());

    private boolean keepRunning = true;
    private long updateInterval;

    private File file;
    private long filePointer = 0;

    private EventHandler eventHandler;

    public FileEventSource(long updateInterval, File file, long filePointer, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.filePointer = filePointer;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (this.keepRunning) {
                Thread.sleep(this.updateInterval);

                long len = this.file.length();

                if (len < this.filePointer) {
                    logger.info("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if (len > this.filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");

    }
}
