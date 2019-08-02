package com.cl.graph.weibo.core.linux;

import org.springframework.util.DigestUtils;

import java.util.HashMap;
import java.util.Map;

public class LinuxUtil {

    private static Map<String, LinuxAction> map = new HashMap<>(8);

    private static String getMd5(String ipAddr, int port , String user) {
        return DigestUtils.md5DigestAsHex((ipAddr + "~" + port + "~" + user).getBytes());
    }

    public static LinuxAction getSingletonLinuxAction(String ip, String user, String password) {
        return getSingletonLinuxAction(ip, LinuxAction.DEFAULT_PORT, user, password, LinuxAction.DEFAULT_CHART);
    }

    public static LinuxAction getSingletonLinuxAction(String ip, int port, String user, String password) {
        return getSingletonLinuxAction(ip, port, user, password, LinuxAction.DEFAULT_CHART);
    }

    public static LinuxAction getSingletonLinuxAction(String ip, String user, String password, String charset) {
        return getSingletonLinuxAction(ip, LinuxAction.DEFAULT_PORT, user, password, charset);

    }

    private static LinuxAction getSingletonLinuxAction(String ip, int port, String user, String password, String charset) {
        String md5Str = getMd5(ip, port, user);
        LinuxAction linuxAction = map.get(md5Str);
        if (linuxAction == null) {
            synchronized (LinuxUtil.class) {
                if (linuxAction == null) {
                    linuxAction = new LinuxAction(ip, user, password, port, charset);
                    map.put(md5Str, linuxAction);
                }
            }
        }
        linuxAction.setIp(ip).setPort(port).setUser(user).setPassword(password).setCharset(charset);
        return linuxAction;
    }


}
