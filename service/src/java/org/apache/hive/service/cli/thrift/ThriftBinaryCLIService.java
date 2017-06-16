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

package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;


public class ThriftBinaryCLIService extends ThriftCLIService {
  private final Runnable oomHook;

  public ThriftBinaryCLIService(CLIService cliService, Runnable oomHook) {
    super(cliService, ThriftBinaryCLIService.class.getSimpleName());
    this.oomHook = oomHook;
  }

  @Override
  public void run() {
    try {
      // Server thread pool, 定义处理请求的线程池
      String threadPoolName = "HiveServer2-Handler-Pool";
      // 这里的ExecutorService线程池的定义中用了SynchronousQueue队列，该队列的可以认为是长度为1的阻塞队列，当线程池满，并且没有空闲线程，便会阻塞。
      ExecutorService executorService = new ThreadPoolExecutorWithOomHook(minWorkerThreads,
          maxWorkerThreads, workerKeepAliveTime, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new ThreadFactoryWithGarbageCleanup(threadPoolName),
          oomHook);

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      // 返回TSaslServerTransport.Factory实例
      TTransportFactory transportFactory = hiveAuthFactory.getAuthTransFactory();
      // 返回SQLPlainProcessorFactory实例(调用了SQLPlainProcessorFactory的构造方法, 构造方法入参为this(ThriftBinaryCLIService)), 参看SQLPlainProcessorFactory的getProcessor方法
      TProcessorFactory processorFactory = hiveAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      // HIVE_SSL_PROTOCOL_BLACKLIST默认值为SSLv2,SSLv3
      for (String sslVersion : hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }
      // HIVE_SERVER2_USE_SSL默认值为false
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        // hiveHost和portNum在父类ThriftCLIService的init方法中已经初始化过, host默认是本机地址, portNum默认是10000端口
        serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum);  // 设置ip和端口号
      } else {
        String keyStorePath = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname
              + " Not configured for SSL connection");
        }
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      // Server args
      // HiveServer2服务器将接受的最大消息大小（以字节为单位）,默认最大值为100MB
      int maxMessageSize = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE);
      // Thrift客户端登录HiveServer2时超时间, 默认是20s
      int requestTimeout = (int) hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, TimeUnit.SECONDS);
      // Thrift客户端默认指数回退时间100ms
      int beBackoffSlotLength = (int) hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, TimeUnit.MILLISECONDS);
      // 构造TThreadPoolServer.Args，上面的线程池会作为参数传入
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)   // serverTransport = serverSocket
          // processorFactory= processorFactory(SQLPlainProcessorFactory) inputTransportFactory/outputTransportFactory=transportFactory(TSaslServerTransport.Factory)
          .processorFactory(processorFactory).transportFactory(transportFactory)
          // inputProtocolFactory/outputProtocolFactory = TBinaryProtocol.Factory()
          .protocolFactory(new TBinaryProtocol.Factory())
          // inputProtocolFactory = TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize) maxMessageSize限制了从可变长度字段(如字符串或二进制)读取的最大字节数
          // maxMessageSize设置从容器类如maps, sets, lists, 读取的最大元素数量
          .inputProtocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
          .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS)
          .beBackoffSlotLength(beBackoffSlotLength).beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
          .executorService(executorService);

      // TCP Server
      // TThreadPoolServer的特点是，客户端只要不从服务器上断开连接，就会一直占据服务器的一个线程，所以可能会出现阻塞问题
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(new TServerEventHandler() {
        @Override
        public ServerContext createContext(
          TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.incrementCounter(MetricsConstant.OPEN_CONNECTIONS);
              metrics.incrementCounter(MetricsConstant.CUMULATIVE_CONNECTION_COUNT);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
          return new ThriftCLIServerContext();
        }

        @Override
        public void deleteContext(ServerContext serverContext,
          TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.decrementCounter(MetricsConstant.OPEN_CONNECTIONS);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
          ThriftCLIServerContext context = (ThriftCLIServerContext) serverContext;
          SessionHandle sessionHandle = context.getSessionHandle();
          if (sessionHandle != null) {
            LOG.info("Session disconnected without closing properly. ");
            try {
              boolean close = cliService.getSessionManager().getSession(sessionHandle).getHiveConf()
                .getBoolVar(ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT);
              LOG.info((close ? "" : "Not ") + "Closing the session: " + sessionHandle);
              if (close) {
                cliService.closeSession(sessionHandle);
              }
            } catch (HiveSQLException e) {
              LOG.warn("Failed to close session: " + e, e);
            }
          }
        }

        @Override
        public void preServe() {
        }

        @Override
        public void processContext(ServerContext serverContext,
          TTransport input, TTransport output) {
          currentServerContext.set(serverContext);
        }
      });
      String msg = "Starting " + ThriftBinaryCLIService.class.getSimpleName() + " on port "
          + portNum + " with " + minWorkerThreads + "..." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      // 启动Thrift Server
      server.serve();
    } catch (Throwable t) {
      LOG.error(
          "Error starting HiveServer2: could not start "
              + ThriftBinaryCLIService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

}
