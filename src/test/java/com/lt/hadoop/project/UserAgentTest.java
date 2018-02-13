package com.lt.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.SocketPermission;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by taoshiliu on 2018/2/13.
 */
public class UserAgentTest {

    @Test
    public void testReadFile() throws Exception {
        String path = "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));

        String line = "";
        UserAgentParser userAgentParser  = new UserAgentParser();

        Map<String,Integer> browserMap = new HashMap<String,Integer>();

        while (line != null) {
            line = reader.readLine();
            if(StringUtils.isNotBlank(line)) {

                String source = line.substring(getCharacterPosition(line,"\"",7) + 1);
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String plateform = agent.getPlatform();
                boolean isMoblie = agent.isMobile();

                Integer browserValue = browserMap.get(browser);
                if(browserMap.get(browser) != null) {
                    browserMap.put(browser,browserMap.get(browser) + 1);
                }else {
                    browserMap.put(browser,1);
                }
            }
        }

        for(Map.Entry<String,Integer> entry : browserMap.entrySet()) {
            System.out.print(entry.getKey() + " : " + entry.getValue());
        }
    }

    private int getCharacterPosition(String value,String operator,int index) {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while(slashMatcher.find()) {
            mIdx++;

            if(mIdx == index) {
                break;
            }
        }
        return slashMatcher.start();
    }

    /*public static void main(String[] args) {
        //开源userAgent解析程序
        String source = "";
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
    }*/

}
