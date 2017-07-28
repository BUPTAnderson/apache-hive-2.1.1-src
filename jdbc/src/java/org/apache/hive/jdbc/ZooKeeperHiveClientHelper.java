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

package org.apache.hive.jdbc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ZooKeeperHiveClientHelper {
  static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHiveClientHelper.class.getName());
  // Pattern for key1=value1;key2=value2
  private static final Pattern kvPattern = Pattern.compile("([^=;]*)=([^;]*)[;]?");
  /**
   * A no-op watcher class
   */
  static class DummyWatcher implements Watcher {
    @Override
    public void process(org.apache.zookeeper.WatchedEvent event) {
    }
  }

  static void configureConnParams(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException {
    // 获得zooKeeperEnsemble, 例如: zkNode1:2181,zkNode2:2181,zkNode3:2181, 也即是hive-site.xml中hive.zookeeper.quorum的值
    String zooKeeperEnsemble = connParams.getZooKeeperEnsemble();
    // 获取zooKeeperNamespace, 比如: hiveserver2_zk, 也即是hive-site.xml中hive.server2.zookeeper.namespace的值
    String zooKeeperNamespace =
        connParams.getSessionVars().get(JdbcConnectionParams.ZOOKEEPER_NAMESPACE);
    // 如果为null或为空, 设置为默认的namespace: hiveserver2
    if ((zooKeeperNamespace == null) || (zooKeeperNamespace.isEmpty())) {
      zooKeeperNamespace = JdbcConnectionParams.ZOOKEEPER_DEFAULT_NAMESPACE;
    }
    List<String> serverHosts;
    Random randomizer = new Random();
    String serverNode;
    // 构造zooKeeperClient
    CuratorFramework zooKeeperClient =
        CuratorFrameworkFactory.builder().connectString(zooKeeperEnsemble)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    try {
      // 启动client
      zooKeeperClient.start();
      serverHosts = zooKeeperClient.getChildren().forPath("/" + zooKeeperNamespace);
      // Remove the znodes we've already tried from this list
      // 移除不需要的节点, 即移除丢弃的hiveserve2节点, 默认为空
      serverHosts.removeAll(connParams.getRejectedHostZnodePaths());
      if (serverHosts.isEmpty()) {
        throw new ZooKeeperHiveClientException(
            "Tried all existing HiveServer2 uris from ZooKeeper.");
      }
      // Now pick a server node randomly
      // 一般配置了几个hiveServer2到zookeeper, serverHosts.size()就为多少, 这里是随机选择一个hiveServer2节点
      // 比如我们配置了两个节点, 分别是: serverUri=BDS-TEST-002:10000;version=1.2.1;sequence=0000000019 serverUri=BDS-TEST-001:10000;version=1.2.1;sequence=0000000025
      serverNode = serverHosts.get(randomizer.nextInt(serverHosts.size()));
      // 将选中的hiveserver2节点设置给currentHostZnodePath
      connParams.setCurrentHostZnodePath(serverNode);
      // Read data from the znode for this server node
      // This data could be either config string (new releases) or server end
      // point (old releases)
      // 这里比如我们获取的是BDS-TEST-002对应的节点, dataStr值为:
      /*
        BDS-TEST-002:10000
        cZxid = 0xa002ea844
        ctime = Fri Dec 23 11:39:25 CST 2016
        mZxid = 0xa002ea844
        mtime = Fri Dec 23 11:39:25 CST 2016
        pZxid = 0xa002ea844
        cversion = 0
        dataVersion = 0
        aclVersion = 0
        ephemeralOwner = 0x258d78d9ed70018
        dataLength = 18
        numChildren = 0
      */
      String dataStr =
          new String(
              zooKeeperClient.getData().forPath("/" + zooKeeperNamespace + "/" + serverNode),
              Charset.forName("UTF-8"));
      Matcher matcher = kvPattern.matcher(dataStr);
      // If dataStr is not null and dataStr is not a KV pattern,
      // it must be the server uri added by an older version HS2
      if ((dataStr != null) && (!matcher.find())) {
        String[] split = dataStr.split(":");
        if (split.length != 2) {
          throw new ZooKeeperHiveClientException("Unable to read HiveServer2 uri from ZooKeeper: "
              + dataStr);
        }
        connParams.setHost(split[0]);
        connParams.setPort(Integer.parseInt(split[1]));
      } else {
        // 对dataStr中的key value对进行分析设置给connParams
        applyConfs(dataStr, connParams);
      }
    } catch (Exception e) {
      throw new ZooKeeperHiveClientException("Unable to read HiveServer2 configs from ZooKeeper", e);
    } finally {
      // Close the client connection with ZooKeeper
      if (zooKeeperClient != null) {
        zooKeeperClient.close();
      }
    }
  }

  /**
   * Apply configs published by the server. Configs specified from client's JDBC URI override
   * configs published by the server.
   *
   * @param serverConfStr
   * @param connParams
   * @throws Exception
   */
  private static void applyConfs(String serverConfStr, JdbcConnectionParams connParams)
      throws Exception {
    Matcher matcher = kvPattern.matcher(serverConfStr);
    while (matcher.find()) {
      // Have to use this if-else since switch-case on String is supported Java 7 onwards
      if ((matcher.group(1) != null)) {
        if ((matcher.group(2) == null)) {
          throw new Exception("Null config value for: " + matcher.group(1)
              + " published by the server.");
        }
        // Set host
        if (matcher.group(1).equals("hive.server2.thrift.bind.host")) {
          connParams.setHost(matcher.group(2));
        }
        // Set transportMode
        if ((matcher.group(1).equals("hive.server2.transport.mode"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.TRANSPORT_MODE))) {
          connParams.getSessionVars().put(JdbcConnectionParams.TRANSPORT_MODE, matcher.group(2));
        }
        // Set port
        if (matcher.group(1).equals("hive.server2.thrift.port")) {
          connParams.setPort(Integer.parseInt(matcher.group(2)));
        }
        if ((matcher.group(1).equals("hive.server2.thrift.http.port"))
            && !(connParams.getPort() > 0)) {
          connParams.setPort(Integer.parseInt(matcher.group(2)));
        }
        // Set sasl qop
        if ((matcher.group(1).equals("hive.server2.thrift.sasl.qop"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_QOP))) {
          connParams.getSessionVars().put(JdbcConnectionParams.AUTH_QOP, matcher.group(2));
        }
        // Set http path
        if ((matcher.group(1).equals("hive.server2.thrift.http.path"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.HTTP_PATH))) {
          connParams.getSessionVars().put(JdbcConnectionParams.HTTP_PATH, matcher.group(2));
        }
        // Set SSL
        if ((matcher.group(1) != null) && (matcher.group(1).equals("hive.server2.use.SSL"))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.USE_SSL))) {
          connParams.getSessionVars().put(JdbcConnectionParams.USE_SSL, matcher.group(2));
        }
        /**
         * Note: this is pretty messy, but sticking to the current implementation.
         * Set authentication configs. Note that in JDBC driver, we have 3 auth modes: NOSASL,
         * Kerberos (including delegation token mechanism) and password based.
         * The use of JdbcConnectionParams.AUTH_TYPE==JdbcConnectionParams.AUTH_SIMPLE picks NOSASL.
         * The presence of JdbcConnectionParams.AUTH_PRINCIPAL==<principal> picks Kerberos.
         * If principal is absent, the presence of
         * JdbcConnectionParams.AUTH_TYPE==JdbcConnectionParams.AUTH_TOKEN uses delegation token.
         * Otherwise password based (which includes NONE, PAM, LDAP, CUSTOM)
         */
        if (matcher.group(1).equals("hive.server2.authentication")) {
          // NOSASL
          if (matcher.group(2).equalsIgnoreCase("NOSASL")
              && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_TYPE) && connParams
                  .getSessionVars().get(JdbcConnectionParams.AUTH_TYPE)
                  .equalsIgnoreCase(JdbcConnectionParams.AUTH_SIMPLE))) {
            connParams.getSessionVars().put(JdbcConnectionParams.AUTH_TYPE,
                JdbcConnectionParams.AUTH_SIMPLE);
          }
        }
        // KERBEROS
        // If delegation token is passed from the client side, do not set the principal
        if (matcher.group(1).equalsIgnoreCase("hive.server2.authentication.kerberos.principal")
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_TYPE) && connParams
                .getSessionVars().get(JdbcConnectionParams.AUTH_TYPE)
                .equalsIgnoreCase(JdbcConnectionParams.AUTH_TOKEN))
            && !(connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_PRINCIPAL))) {
          connParams.getSessionVars().put(JdbcConnectionParams.AUTH_PRINCIPAL, matcher.group(2));
        }
      }
    }
  }
}
