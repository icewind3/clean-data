package com.cl.graph.weibo.core.linux;

import ch.ethz.ssh2.*;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.exception.BusinessException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.io.*;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public class LinuxAction {

    public final static String LINUX_SCRIPT_FILE_DIR = "/home/cldev/script";
    public final static Integer DEFAULT_PORT = 22;
    public final static String DEFAULT_CHART = "UTF-8";
    public final static String AUTHENTICATION_CODE = "authentication";
    public final static String CONNECT_CODE = "connect";

    private Connection conn;
    private String ip;
    private String user;
    private String password;
    private int port;
    private String charset;

    public LinuxAction(String ip, String user, String password, int port, String charset) {
        this.ip = ip;
        this.user = user;
        this.password = password;
        this.port = port;
        this.charset = charset;
        this.conn = new Connection(ip, port);
    }

    /**
     * 执行一条Linux语句
     *
     * @param cmd Linux语句
     * @return 返回格式封装对象
     */
    public ResultApi<List<String>> executeSuccess(String cmd) {
        return this.executeSuccess(cmd, null);
    }

    public ResultApi<List<String>> executeSuccess(String cmd, ResultApiCallInterface<List<String>> resultApiCallInterface) {
        Session session = null;
        try {
            if (login()) {
                session = conn.openSession();
                session.execCommand(cmd);
                return processStdout(session.getStdout(), resultApiCallInterface);
            } else {
                throw new BusinessException("authentication");
            }
        } catch (IOException e) {
            if (e instanceof SocketException) {
                return ResultApi.build(500, e.getMessage(), null);
            } else {
                e.printStackTrace();
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
        return ResultApi.build(500, "服务器异常", null);
    }

    public ResultApi downloadFile(String remoteFile, String remoteTargetDirectory, String newPath) {
        try {
            if (login()) {
                SCPClient scpClient = conn.createSCPClient();
                SCPInputStream sis = scpClient.get(remoteTargetDirectory + File.separator + remoteFile);
                File f = new File(newPath);
                if (!f.exists()) {
                    f.mkdirs();
                }
                File newFile = new File(newPath + remoteFile);
                FileOutputStream fos = new FileOutputStream(newFile);
                byte[] b = new byte[4096];
                int i;
                while ((i = sis.read(b)) != -1) {
                    fos.write(b, 0, i);
                }
                fos.flush();
                fos.close();
                sis.close();
                conn.close(new Throwable("Closed due to user request."), true);
                return ResultApi.ok();
            } else {
                return ResultApi.build(500, "连接建立失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResultApi.build(500, "下载失败");
    }

    /**
     * 上传文件到linux的指定目录下
     *
     * @param file                  文件对象
     * @param remoteTargetDirectory linux的目录位置
     * @param mode
     * @return 返回格式封装对象
     */
    public ResultApi uploadFile(File file, String remoteTargetDirectory, String mode) {
        try {
            if (!login()) {
                return ResultApi.build(500, "连接建立失败");
            }
            SCPClient scpClient = new SCPClient(conn);
            SCPOutputStream os = scpClient.put(file.getName(), file.length(), remoteTargetDirectory, mode);
            byte[] b = new byte[4096];
            FileInputStream fis = new FileInputStream(file);
            int i;
            while ((i = fis.read(b)) != -1) {
                os.write(b, 0, i);
            }
            os.flush();
            fis.close();
            os.close();
            conn.close(new Throwable("Closed due to user request."), true);
            return ResultApi.ok();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResultApi.build(500, file.getPath() + "上传失败");
    }

    /**
     * 上传文件夹到linux的指定目录下
     *
     * @param fileDir               文件目录对象
     * @param remoteTargetDirectory linux的目录位置
     * @param mode
     * @return 返回格式封装对象
     */
    public ResultApi uploadDirectory(File fileDir, String remoteTargetDirectory, String mode) {
        if (fileDir.isDirectory()) {
            List<File> fileList = new ArrayList<>();
            File[] files = fileDir.listFiles();
            if (files != null && files.length > 0) {
                fileList.addAll(Arrays.asList(files));
            }
            ResultApi<List<String>> executeResultApi = this.executeSuccess("mkdir -p " + remoteTargetDirectory
                    + CommonConstants.FORWARD_SLASH + fileDir.getName());
            if (!executeResultApi.isRel()) {
                return executeResultApi;
            }

            for (File file : fileList) {
                ResultApi resultApi = this.uploadFile(file, remoteTargetDirectory
                        + CommonConstants.FORWARD_SLASH + fileDir.getName(), mode);
                if (!resultApi.isRel()) {
                    return resultApi;
                }
            }
            return ResultApi.ok();
        } else {
            return this.uploadFile(fileDir, remoteTargetDirectory, mode);
        }
    }

    public ResultApi uploadFile(InputStream is, String fileName, long length, String remoteTargetDirectory, String mode) {
        try {
            if (!login()) {
                return ResultApi.build(500, "连接建立失败");
            }
            SCPClient scpClient = new SCPClient(conn);
            SCPOutputStream os = scpClient.put(fileName, length, remoteTargetDirectory, mode);
            byte[] b = new byte[4096];
            int i;
            while ((i = is.read(b)) != -1) {
                os.write(b, 0, i);
            }
            os.flush();
            is.close();
            os.close();
            conn.close(new Throwable("Closed due to user request."), true);
            return ResultApi.ok();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResultApi.build(500, "上传失败");
    }

    public SCPInputStream getInputStream(String remoteFile, String remoteTargetDirectory) throws IOException {
        if (login()) {
            SCPClient scpClient = conn.createSCPClient();
            String removeFilePath = remoteTargetDirectory + File.separator + remoteFile;
            return scpClient.get(removeFilePath.replaceAll("\\\\", CommonConstants.FORWARD_SLASH));
        }
        throw new IOException("login fail");
    }


    /**
     * 判断是否可以操作远端
     *
     * @return
     * @throws IOException
     */
    private synchronized Boolean login() throws IOException {
        if (!connectIp()) {
            return false;
        }
        boolean flg;
        connect();
        flg = isAuthentication();
        if (!flg) {
            flg = conn.authenticateWithPassword(user, password);
        }
        return flg;
    }

    /**
     * 创建连接
     *
     * @throws IOException
     */
    private synchronized void connect() throws IOException {
        if (isConnection()) {
            return;
        }
        conn.connect();
    }

    /**
     * 判断远程ip是否能连接
     *
     * @return
     */
    private boolean connectIp() throws IOException {
        try {
            InetAddress inetAddress = Inet4Address.getByName(ip);
            if (inetAddress.isReachable(1500)) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IOException("connect ip failed");
    }

    /**
     * 判断是否需要连接
     * 如果连接对象connectionClosed状态已经是未连接状态就重新实例化对象
     *
     * @return
     */
    private synchronized Boolean isConnection() {
        try {
            Field tm = conn.getClass().getDeclaredField("tm");
            tm.setAccessible(true);
            Object o = tm.get(conn);
            if (o == null) {
                return false;
            }
            Field connectionClosed = o.getClass().getSuperclass().getDeclaredField("connectionClosed");
            connectionClosed.setAccessible(true);
            boolean aBoolean = connectionClosed.getBoolean(o);
            if (aBoolean) {
                conn = new Connection(ip, port);
            }
            return !aBoolean;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 返回连接对象中身份校验的状态
     *
     * @return
     */
    private synchronized Boolean isAuthentication() {
        try {
            Field authenticated = conn.getClass().getDeclaredField("authenticated");
            authenticated.setAccessible(true);
            return authenticated.getBoolean(conn);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return false;
    }

    private ResultApi<List<String>> processStdout(InputStream in, ResultApiCallInterface<List<String>> resultApiCallInterface) {
        InputStream stdout = new StreamGobbler(in);
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(stdout, LinuxAction.DEFAULT_CHART));
            if (resultApiCallInterface == null) {
                String line;
                List<String> list = new LinkedList<>();
                while ((line = br.readLine()) != null) {
                    list.add(line);
                }
                br.close();
                return ResultApi.ok(list);
            } else {
                return resultApiCallInterface.run(br);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ResultApi.build(500, "数据读取异常", null);
    }


    private SCPOutputStream customPut(SCPClient scp, final String remoteFile, long length, String remoteTargetDirectory, String mode) throws IOException {
        Session sess;
        if (null == remoteFile) {
            throw new IllegalArgumentException("Null argument.");
        }
        if (null == remoteTargetDirectory) {
            remoteTargetDirectory = "";
        }
        if (null == mode) {
            mode = "0600";
        }
        if (mode.length() != 4) {
            throw new IllegalArgumentException("Invalid mode.");
        }
        for (int i = 0; i < mode.length(); i++) {
            if (!Character.isDigit(mode.charAt(i))) {
                throw new IllegalArgumentException("Invalid mode.");
            }
        }
        remoteTargetDirectory = (remoteTargetDirectory.length() > 0) ? remoteTargetDirectory : ".";
        String cmd = "scp -t -d \"" + remoteTargetDirectory + "\"";
        sess = conn.openSession();
        sess.execCommand(cmd, LinuxAction.DEFAULT_CHART);
        return new SCPOutputStream(scp, sess, remoteFile, length, mode);
    }

}
