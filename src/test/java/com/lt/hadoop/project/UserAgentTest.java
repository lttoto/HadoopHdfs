package com.lt.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

/**
 * Created by taoshiliu on 2018/2/13.
 */
public class UserAgentTest {

    public static void main(String[] args) {
        //开源userAgent解析程序
        String source = "";
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
    }

}
