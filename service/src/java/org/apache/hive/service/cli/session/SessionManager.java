/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.service.cli.session;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hive.service.CompositeService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  public static final String HIVERCFILE = ".hiverc";
  private static final Logger LOG = LoggerFactory.getLogger(CompositeService.class);
  private HiveConf hiveConf;
  // 目前的连接信息，value为HiveSession类，里面大致是用户名、密码、hive配置等info。
  private final Map<SessionHandle, HiveSession> handleToSession =
      new ConcurrentHashMap<SessionHandle, HiveSession>();
  private final OperationManager operationManager = new OperationManager();
  private ThreadPoolExecutor backgroundOperationPool;
  private boolean isOperationLogEnabled;
  private File operationLogRootDir;

  private long checkInterval;
  private long sessionTimeout;
  private boolean checkOperation;

  private volatile boolean shutdown;
  // The HiveServer2 instance running this service
  private final HiveServer2 hiveServer2;
  private String sessionImplWithUGIclassName;
  private String sessionImplclassName;

  public SessionManager(HiveServer2 hiveServer2) {
    super(SessionManager.class.getSimpleName());
    this.hiveServer2 = hiveServer2;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    //Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      // 在本地创建存放Log的文件夹
      initOperationLogRootDir();
    }
    // 创建线程池, 用来执行任务
    createBackgroundOperationPool();
    addService(operationManager);
    initSessionImplClassName();
    // 调用operationManager的init方法
    super.init(hiveConf);
  }

  private void initSessionImplClassName() {
    this.sessionImplclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_CLASSNAME); // 默认值为null
    this.sessionImplWithUGIclassName = hiveConf.getVar(ConfVars.HIVE_SESSION_IMPL_WITH_UGI_CLASSNAME);  // 默认值为null
  }

  private void createBackgroundOperationPool() {
    // 默认值为100
    int poolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS);
    LOG.info("HiveServer2: Background operation thread pool size: " + poolSize);
    // 默认值为100
    int poolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE);
    LOG.info("HiveServer2: Background operation thread wait queue size: " + poolQueueSize);
    long keepAliveTime = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, TimeUnit.SECONDS);
    LOG.info(
        "HiveServer2: Background operation thread keepalive time: " + keepAliveTime + " seconds");

    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    String threadPoolName = "HiveServer2-Background-Pool";
    final BlockingQueue queue = new LinkedBlockingQueue<Runnable>(poolQueueSize);
    // 创建线程池
    backgroundOperationPool = new ThreadPoolExecutor(poolSize, poolSize,
        keepAliveTime, TimeUnit.SECONDS, queue,
        new ThreadFactoryWithGarbageCleanup(threadPoolName));
    backgroundOperationPool.allowCoreThreadTimeOut(true);

    checkInterval = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
    sessionTimeout = HiveConf.getTimeVar(
        hiveConf, ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
    checkOperation = HiveConf.getBoolVar(hiveConf,
        ConfVars.HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION); // 默认值为true
    // 获取metrics实例，添加metrics信息
    Metrics m = MetricsFactory.getInstance();
    if (m != null) {
      m.addGauge(MetricsConstant.EXEC_ASYNC_QUEUE_SIZE, new MetricsVariable() {
        @Override
        public Object getValue() {
          return queue.size();
        }
      });
      m.addGauge(MetricsConstant.EXEC_ASYNC_POOL_SIZE, new MetricsVariable() {
        @Override
        public Object getValue() {
          return backgroundOperationPool.getPoolSize();
        }
      });
    }
  }

  private void initOperationLogRootDir() {
    operationLogRootDir = new File(
        hiveConf.getVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION));
    isOperationLogEnabled = true;

    if (operationLogRootDir.exists() && !operationLogRootDir.isDirectory()) {
      LOG.warn("The operation log root directory exists, but it is not a directory: " +
          operationLogRootDir.getAbsolutePath());
      isOperationLogEnabled = false;
    }

    if (!operationLogRootDir.exists()) {
      // 创建文件夹, 注意这里是mkdirs(), 是级联创建, 先创建/tmp, 再创建用户目录, 比如/tmp/hadoop, 再创建log目录, /tmp/hadoop/operation_logs
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }

    if (isOperationLogEnabled) {
      LOG.info("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath());
      try {
        FileUtils.forceDeleteOnExit(operationLogRootDir);
      } catch (IOException e) {
        LOG.warn("Failed to schedule cleanup HS2 operation logging root dir: " +
            operationLogRootDir.getAbsolutePath(), e);
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    if (checkInterval > 0) {
      startTimeoutChecker();
    }
  }

  private void startTimeoutChecker() {
    final long interval = Math.max(checkInterval, 3000l);  // minimum 3 seconds
    Runnable timeoutChecker = new Runnable() {
      @Override
      public void run() {
        for (sleepInterval(interval); !shutdown; sleepInterval(interval)) {
          long current = System.currentTimeMillis();
          for (HiveSession session : new ArrayList<HiveSession>(handleToSession.values())) {
            if (sessionTimeout > 0 && session.getLastAccessTime() + sessionTimeout <= current
                && (!checkOperation || session.getNoOperationTime() > sessionTimeout)) {
              SessionHandle handle = session.getSessionHandle();
              LOG.warn("Session " + handle + " is Timed-out (last access : " +
                  new Date(session.getLastAccessTime()) + ") and will be closed");
              try {
                closeSession(handle);
              } catch (HiveSQLException e) {
                LOG.warn("Exception is thrown closing session " + handle, e);
              }
            } else {
              session.closeExpiredOperations();
            }
          }
        }
      }

      private void sleepInterval(long interval) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    };
    backgroundOperationPool.execute(timeoutChecker);
  }

  @Override
  public synchronized void stop() {
    super.stop();
    shutdown = true;
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      long timeout = hiveConf.getTimeVar(
          ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e);
      }
      backgroundOperationPool = null;
    }
    cleanupLoggingRootDir();
  }

  private void cleanupLoggingRootDir() {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup root dir of HS2 logging: " + operationLogRootDir
            .getAbsolutePath(), e);
      }
    }
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
      Map<String, String> sessionConf) throws HiveSQLException {
    return openSession(protocol, username, password, ipAddress, sessionConf, false, null);
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in HiveSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   * @see org.apache.hive.service.cli.thrift.ThriftCLIService#getUserName(TOpenSessionReq)
   *
   * @param protocol
   * @param username
   * @param password
   * @param ipAddress
   * @param sessionConf
   * @param withImpersonation
   * @param delegationToken
   * @return
   * @throws HiveSQLException
   */
  public SessionHandle openSession(TProtocolVersion protocol, String username, String password, String ipAddress,
      Map<String, String> sessionConf, boolean withImpersonation, String delegationToken)
          throws HiveSQLException {
    return createSession(null, protocol, username, password, ipAddress, sessionConf,
      withImpersonation, delegationToken).getSessionHandle();
  }
  public HiveSession createSession(SessionHandle sessionHandle, TProtocolVersion protocol, String username,
    String password, String ipAddress, Map<String, String> sessionConf, boolean withImpersonation,
    String delegationToken)
    throws HiveSQLException {

    HiveSession session;
    // If doAs is set to true for HiveServer2, we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    // doAs设置为true的话, withImpersonation为true, 这里我们按false来处理
    if (withImpersonation) {
      HiveSessionImplwithUGI hiveSessionUgi;
      if (sessionImplWithUGIclassName == null) {
        hiveSessionUgi = new HiveSessionImplwithUGI(sessionHandle, protocol, username, password,
            hiveConf, ipAddress, delegationToken);
      } else {
        try {
          Class<?> clazz = Class.forName(sessionImplWithUGIclassName);
          Constructor<?> constructor = clazz.getConstructor(SessionHandle.class, TProtocolVersion.class, String.class,
            String.class, HiveConf.class, String.class, String.class);
          hiveSessionUgi = (HiveSessionImplwithUGI) constructor.newInstance(sessionHandle,
              protocol, username, password, hiveConf, ipAddress, delegationToken);
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initilize session class:" + sessionImplWithUGIclassName);
        }
      }
      session = HiveSessionProxy.getProxy(hiveSessionUgi, hiveSessionUgi.getSessionUgi());
      hiveSessionUgi.setProxySession(session);
    } else {
      // sessionImplclassName默认值为null
      if (sessionImplclassName == null) {
        // 这里session是HiveSessionImpl, hiveConf是SessionManager的属性, 在HiveSessionImpl的构造方法中会复制一份该hiveConf来作HiveSessionImpl的sessionConf
        session = new HiveSessionImpl(sessionHandle, protocol, username, password, hiveConf,
          ipAddress);
      } else {
        try {
        Class<?> clazz = Class.forName(sessionImplclassName);
        Constructor<?> constructor = clazz.getConstructor(SessionHandle.class, TProtocolVersion.class,
          String.class, String.class, HiveConf.class, String.class);
        session = (HiveSession) constructor.newInstance(sessionHandle, protocol, username, password,
          hiveConf, ipAddress);
        } catch (Exception e) {
          throw new HiveSQLException("Cannot initilize session class:" + sessionImplclassName, e);
        }
      }
    }
    session.setSessionManager(this);
    // 重要的一步, 将operationManager设置给HiveSessionImpl的operationManager， 以后具体的hql的执行需要由operationManager来执行
    session.setOperationManager(operationManager);
    try {
      // 调用HiveSessionImpl的open方法, 这里的sessionConf是req.getConfiguration(), 即从调用端传过来的， 非常重要的一个方法， 该方法会为当前的connect创建一个HiveSerer2 session --> SessionState
      session.open(sessionConf);
    } catch (Exception e) {
      LOG.warn("Failed to open session", e);
      try {
        session.close();
      } catch (Throwable t) {
        LOG.warn("Error closing session", t);
      }
      session = null;
      throw new HiveSQLException("Failed to open new session: " + e.getMessage(), e);
    }
    // 初始化SessionManager的过程中该值为true
    if (isOperationLogEnabled) {
      // operationLogRootDir默认值为:"${system:java.io.tmpdir}/${system:user.name}/operation_logs", 比如: /tmp/hadoop/operation_logs/, 调用HiveSessionImpl的setOperationLogSessionDir方法
      session.setOperationLogSessionDir(operationLogRootDir);
    }
    try {
      executeSessionHooks(session);
    } catch (Exception e) {
      LOG.warn("Failed to execute session hooks", e);
      try {
        session.close();
      } catch (Throwable t) {
        LOG.warn("Error closing session", t);
      }
      session = null;
      throw new HiveSQLException("Failed to execute session hooks: " + e.getMessage(), e);
    }
    // 重要的一步, 将<SessionHandle, session>放入到handleToSession, 即session是可以复用的
    handleToSession.put(session.getSessionHandle(), session);
    // 打印日志, 比如: INFO [HiveServer2-Handler-Pool: Thread-212] service.CompositeService: Session opened, SessionHandle [74669a05-ba6e-484d-b6d5-b255b8e361c8], current sessions:1
    LOG.info("Session opened, " + session.getSessionHandle() + ", current sessions:" + getOpenSessionCount());
    return session;
  }

  public synchronized void closeSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Session does not exist!");
    }
    LOG.info("Session closed, " + sessionHandle + ", current sessions:" + getOpenSessionCount());
    try {
      session.close();
    } finally {
      // Shutdown HiveServer2 if it has been deregistered from ZooKeeper and has no active sessions
      if (!(hiveServer2 == null) && (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY))
          && (hiveServer2.isDeregisteredWithZooKeeper())) {
        // Asynchronously shutdown this instance of HiveServer2,
        // if there are no active client sessions
        if (getOpenSessionCount() == 0) {
          LOG.info("This instance of HiveServer2 has been removed from the list of server "
              + "instances available for dynamic service discovery. "
              + "The last client session has ended - will shutdown now.");
          Thread shutdownThread = new Thread() {
            @Override
            public void run() {
              hiveServer2.stop();
            }
          };
          shutdownThread.start();
        }
      }
    }
  }

  public HiveSession getSession(SessionHandle sessionHandle) throws HiveSQLException {
    HiveSession session = handleToSession.get(sessionHandle);
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>();

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<List<String>> threadLocalForwardedAddresses = new ThreadLocal<List<String>>();

  public static void setForwardedAddresses(List<String> ipAddress) {
    threadLocalForwardedAddresses.set(ipAddress);
  }

  public static void clearForwardedAddresses() {
    threadLocalForwardedAddresses.remove();
  }

  public static List<String> getForwardedAddresses() {
    return threadLocalForwardedAddresses.get();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  public static void clearUserName() {
    threadLocalUserName.remove();
  }

  public static String getUserName() {
    return threadLocalUserName.get();
  }

  private static ThreadLocal<String> threadLocalProxyUserName = new ThreadLocal<String>(){
    @Override
    protected String initialValue() {
      return null;
    }
  };

  public static void setProxyUserName(String userName) {
    LOG.debug("setting proxy user name based on query param to: " + userName);
    threadLocalProxyUserName.set(userName);
  }

  public static String getProxyUserName() {
    return threadLocalProxyUserName.get();
  }

  public static void clearProxyUserName() {
    threadLocalProxyUserName.remove();
  }

  // execute session hooks
  private void executeSessionHooks(HiveSession session) throws Exception {
    List<HiveSessionHook> sessionHooks = HookUtils.getHooks(hiveConf,
        HiveConf.ConfVars.HIVE_SERVER2_SESSION_HOOK, HiveSessionHook.class);
    for (HiveSessionHook sessionHook : sessionHooks) {
      sessionHook.run(new HiveSessionHookContextImpl(session));
    }
  }

  public Future<?> submitBackgroundOperation(Runnable r) {
    return backgroundOperationPool.submit(r);
  }

  public Collection<Operation> getOperations() {
    return operationManager.getOperations();
  }

  public Collection<HiveSession> getSessions() {
    return Collections.unmodifiableCollection(handleToSession.values());
  }

  public int getOpenSessionCount() {
    return handleToSession.size();
  }
}

