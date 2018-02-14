package com.lt.hadoop.spring;

import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

/**
 * Created by taoshiliu on 2018/2/14.
 */
@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner{

    @Autowired
    FsShell fsShell;

    public void run(String... strings) throws Exception {
        for(FileStatus fileStatus : fsShell.lsr("")) {
            System.out.println(fileStatus.getPath());
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHDFSApp.class,args);
    }
}
