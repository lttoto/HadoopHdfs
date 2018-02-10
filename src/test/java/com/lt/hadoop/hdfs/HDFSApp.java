package com.lt.hadoop.hdfs;

import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by taoshiliu on 2018/2/10.
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://hadoop000:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Test
    public void mkidr() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    @Test
    public void creat() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out,1024);
        in.close();
    }

    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("");
        Path hdfsPath = new Path("");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("XXX")));
        FSDataOutputStream output = fileSystem.create(new Path("yyy"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });

        IOUtils.copyBytes(in,output,4096);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("");
        Path hdfsPath = new Path("");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path(""));
        for(FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path(""),new Boolean(true));
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }


}
