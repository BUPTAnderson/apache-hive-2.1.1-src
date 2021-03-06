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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of the query block.
 * QueryBlock是一条SQL最基本的组成单元，包括三个部分：输入源，计算过程，输出。简单来讲一个QueryBlock就是一个子查询。
 **/

public class QB {

  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.parse.QB");

  private final int numJoins = 0;
  private final int numGbys = 0;
  // 表示TOK_SELECT的个数, 比如1
  private int numSels = 0;
  private int numSelDi = 0;
  // alias -> 库名.表名/表名 的映射, 比如: <"partition_test", "default.partition_test">
  private HashMap<String, String> aliasToTabs;
  // alias -> 子查询的映射, QBExpr又包含QB对象, 也就是说如果有子查询的话, 子查询又有对应的qb。key值为子查询的别名
  private HashMap<String, QBExpr> aliasToSubq;
  private HashMap<String, Table> viewAliasToViewSchema;
  private HashMap<String, Map<String, String>> aliasToProps;
  // 当前包含的别名, 主要是为了判断多个别名不可以重复, 如果没有别名, 加入的是原表名, 比如加入 "partition_test"
  private List<String> aliases;
  // 存放了QB parse的相关信息，保存一个基本SQL单元中的各个操作部分的AST Tree结构
  private QBParseInfo qbp;
  // 里面存储了alias和Table的对应关系, 即meta data。保存每个输入表的元信息，比如表在HDFS上的路径，保存表数据的文件格式等。
  private QBMetaData qbm;
  private QBJoinTree qbjoin; // 是对Join语法树的结构化
  private String id;
  // 是不是query
  private boolean isQuery;
  private boolean isAnalyzeRewrite;
  private CreateTableDesc tblDesc = null; // table descriptor of the final
  private CreateTableDesc directoryDesc = null ;
  private List<Path> encryptedTargetTablePaths;
  private boolean insideView;
  private Set<String> aliasInsideView;

  // used by PTFs
  /*
   * This map maintains the PTFInvocationSpec for each PTF chain invocation in this QB.
   */
  private HashMap<ASTNode, PTFInvocationSpec> ptfNodeToSpec;
  /*
   * the WindowingSpec used for windowing clauses in this QB.
   */
  private HashMap<String, WindowingSpec> destToWindowingSpec;

  /*
   * If this QB represents a  SubQuery predicate then this will point to the SubQuery object.
   */
  private QBSubQuery subQueryPredicateDef;

  /*
   * used to give a unique name to each SubQuery QB Currently there can be at
   * most 2 SubQueries in a Query: 1 in the Where clause, and 1 in the Having
   * clause.
   */
  private int numSubQueryPredicates;

  /*
   * for now a top level QB can have 1 where clause SQ predicate.
   */
  private QBSubQuery whereClauseSubQueryPredicate;

  /*
   * for now a top level QB can have 1 where clause SQ predicate.
   */
  private QBSubQuery havingClauseSubQueryPredicate;

  // results

  public void print(String msg) {
    LOG.info(msg + "alias=" + qbp.getAlias());
    for (String alias : getSubqAliases()) {
      QBExpr qbexpr = getSubqForAlias(alias);
      LOG.info(msg + "start subquery " + alias);
      qbexpr.print(msg + " ");
      LOG.info(msg + "end subquery " + alias);
    }
  }

  public QB() {
  }

  // SemanticAnalyzer中init方法会初始化QB, 传入的参数是: null, null, false
  public QB(String outer_id, String alias, boolean isSubQ) {
    // Must be deterministic order maps - see HIVE-8707
    aliasToTabs = new LinkedHashMap<String, String>();
    aliasToSubq = new LinkedHashMap<String, QBExpr>();
    viewAliasToViewSchema = new LinkedHashMap<String, Table>();
    aliasToProps = new LinkedHashMap<String, Map<String, String>>();
    aliases = new ArrayList<String>();
    if (alias != null) {
      alias = alias.toLowerCase();
    }
    // 初始化qbp, alias是null, isSubQ是false
    qbp = new QBParseInfo(alias, isSubQ);
    qbm = new QBMetaData();
    // Must be deterministic order maps - see HIVE-8707
    ptfNodeToSpec = new LinkedHashMap<ASTNode, PTFInvocationSpec>();
    destToWindowingSpec = new LinkedHashMap<String, WindowingSpec>();
    // id 为null
    id = getAppendedAliasFromId(outer_id, alias);
    aliasInsideView = new HashSet<>();
  }

  // For sub-queries, the id. and alias should be appended since same aliases can be re-used
  // within different sub-queries.
  // For a query like:
  // select ...
  //   (select * from T1 a where ...) subq1
  //  join
  //   (select * from T2 a where ...) subq2
  // ..
  // the alias is modified to subq1:a and subq2:a from a, to identify the right sub-query.
  public static String getAppendedAliasFromId(String outer_id, String alias) {
    return (outer_id == null ? alias : outer_id + ":" + alias);
  }

  public String getAlias() {
    return qbp.getAlias();
  }

  public QBParseInfo getParseInfo() {
    return qbp;
  }

  public QBMetaData getMetaData() {
    return qbm;
  }

  public void setQBParseInfo(QBParseInfo qbp) {
    this.qbp = qbp;
  }

  public void countSelDi() {
    numSelDi++;
  }

  public void countSel() {
    numSels++;
  }

  public boolean exists(String alias) {
    alias = alias.toLowerCase();
    if (aliasToTabs.get(alias) != null || aliasToSubq.get(alias) != null) {
      return true;
    }

    return false;
  }

  public void setTabAlias(String alias, String tabName) {
    aliasToTabs.put(alias.toLowerCase(), tabName);
  }

  public void setSubqAlias(String alias, QBExpr qbexpr) {
    aliasToSubq.put(alias.toLowerCase(), qbexpr);
  }

  public void setTabProps(String alias, Map<String, String> props) {
    aliasToProps.put(alias.toLowerCase(), props);
  }

  public void addAlias(String alias) {
    if (!aliases.contains(alias.toLowerCase())) {
      aliases.add(alias.toLowerCase());
    }
  }

  public String getId() {
    return id;
  }

  public int getNumGbys() {
    return numGbys;
  }

  public int getNumSelDi() {
    return numSelDi;
  }

  public int getNumSels() {
    return numSels;
  }

  public int getNumJoins() {
    return numJoins;
  }

  public Set<String> getSubqAliases() {
    return aliasToSubq.keySet();
  }

  public Set<String> getTabAliases() {
    return aliasToTabs.keySet();
  }

  public List<String> getAliases() {
    return aliases;
  }

  public QBExpr getSubqForAlias(String alias) {
    return aliasToSubq.get(alias.toLowerCase());
  }

  public String getTabNameForAlias(String alias) {
    return aliasToTabs.get(alias.toLowerCase());
  }

  public Map<String, String> getTabPropsForAlias(String alias) {
    return aliasToProps.get(alias.toLowerCase());
  }

  public void rewriteViewToSubq(String alias, String viewName, QBExpr qbexpr, Table tab) {
    alias = alias.toLowerCase();
    String tableName = aliasToTabs.remove(alias);
    assert (viewName.equals(tableName));
    aliasToSubq.put(alias, qbexpr);
    if (tab != null) {
      viewAliasToViewSchema.put(alias, tab);
    }
  }

  public void rewriteCTEToSubq(String alias, String cteName, QBExpr qbexpr) {
    rewriteViewToSubq(alias, cteName, qbexpr, null);
  }

  public QBJoinTree getQbJoinTree() {
    return qbjoin;
  }

  public void setQbJoinTree(QBJoinTree qbjoin) {
    this.qbjoin = qbjoin;
  }

  public void setIsQuery(boolean isQuery) {
    this.isQuery = isQuery;
  }

  /**
   * Set to true in SemanticAnalyzer.getMetadataForDestFile,
   * if destination is a file and query is not CTAS
   * @return
   */
  public boolean getIsQuery() {
    return isQuery;
  }

  // to decide whether to rewrite RR of subquery
  public boolean isTopLevelSelectStarQuery() {
    return !isCTAS() && qbp.isTopLevelSimpleSelectStarQuery();
  }

  // to find target for fetch task conversion optimizer (not allows subqueries)
  public boolean isSimpleSelectQuery() {
    if (!qbp.isSimpleSelectQuery() || isCTAS() || qbp.isAnalyzeCommand()) {
      return false;
    }
    for (QBExpr qbexpr : aliasToSubq.values()) {
      if (!qbexpr.isSimpleSelectQuery()) {
        return false;
      }
    }
    return true;
  }

  public boolean hasTableSample(String alias) {
    return qbp.getTabSample(alias) != null;
  }

  public CreateTableDesc getTableDesc() {
    return tblDesc;
  }

  public void setTableDesc(CreateTableDesc desc) {
    tblDesc = desc;
  }

  public CreateTableDesc getDirectoryDesc() {
    return directoryDesc;
  }

  public void setDirectoryDesc(CreateTableDesc directoryDesc) {
    this.directoryDesc = directoryDesc;
  }

  /**
   * Whether this QB is for a CREATE-TABLE-AS-SELECT.
   */
  public boolean isCTAS() {
    return tblDesc != null;
  }

  /**
   * Retrieve skewed column name for a table.
   * @param alias table alias
   * @return
   */
  public List<String> getSkewedColumnNames(String alias) {
    List<String> skewedColNames = null;
    if (null != qbm &&
        null != qbm.getAliasToTable() &&
            qbm.getAliasToTable().size() > 0) {
      Table tbl = getMetaData().getTableForAlias(alias);
      skewedColNames = tbl.getSkewedColNames();
    }
    return skewedColNames;

  }

  public boolean isAnalyzeRewrite() {
    return isAnalyzeRewrite;
  }

  public void setAnalyzeRewrite(boolean isAnalyzeRewrite) {
    this.isAnalyzeRewrite = isAnalyzeRewrite;
  }

  public PTFInvocationSpec getPTFInvocationSpec(ASTNode node) {
    return ptfNodeToSpec == null ? null : ptfNodeToSpec.get(node);
  }

  public void addPTFNodeToSpec(ASTNode node, PTFInvocationSpec spec) {
    // Must be deterministic order map - see HIVE-8707
    ptfNodeToSpec = ptfNodeToSpec == null ? new LinkedHashMap<ASTNode, PTFInvocationSpec>() : ptfNodeToSpec;
    ptfNodeToSpec.put(node, spec);
  }

  public HashMap<ASTNode, PTFInvocationSpec> getPTFNodeToSpec() {
    return ptfNodeToSpec;
  }

  public WindowingSpec getWindowingSpec(String dest) {
    return destToWindowingSpec.get(dest);
  }

  public void addDestToWindowingSpec(String dest, WindowingSpec windowingSpec) {
    destToWindowingSpec.put(dest, windowingSpec);
  }

  public boolean hasWindowingSpec(String dest) {
    return destToWindowingSpec.get(dest) != null;
  }

  public HashMap<String, WindowingSpec> getAllWindowingSpecs() {
    return destToWindowingSpec;
  }

  protected void setSubQueryDef(QBSubQuery subQueryPredicateDef) {
    this.subQueryPredicateDef = subQueryPredicateDef;
  }

  protected QBSubQuery getSubQueryPredicateDef() {
    return subQueryPredicateDef;
  }

  protected int getNumSubQueryPredicates() {
    return numSubQueryPredicates;
  }

  protected int incrNumSubQueryPredicates() {
    return ++numSubQueryPredicates;
  }

  void setWhereClauseSubQueryPredicate(QBSubQuery sq) {
    whereClauseSubQueryPredicate = sq;
  }

  public QBSubQuery getWhereClauseSubQueryPredicate() {
    return whereClauseSubQueryPredicate;
  }

  void setHavingClauseSubQueryPredicate(QBSubQuery sq) {
    havingClauseSubQueryPredicate = sq;
  }

  public QBSubQuery getHavingClauseSubQueryPredicate() {
    return havingClauseSubQueryPredicate;
  }

  void addEncryptedTargetTablePath(Path p) {
    if(encryptedTargetTablePaths == null) {
      encryptedTargetTablePaths = new ArrayList<>();
    }
    encryptedTargetTablePaths.add(p);
  }
  /**
   * List of dbName.tblName of encrypted target tables of insert statement
   * Used to support Insert ... values(...)
   */
  List<Path> getEncryptedTargetTablePaths() {
    if(encryptedTargetTablePaths == null) {
      return Collections.emptyList();
    }
    return encryptedTargetTablePaths;
  }

  public HashMap<String, Table> getViewToTabSchema() {
    return viewAliasToViewSchema;
  }

  public boolean isInsideView() {
    return insideView;
  }

  public void setInsideView(boolean insideView) {
    this.insideView = insideView;
  }

  public Set<String> getAliasInsideView() {
    return aliasInsideView;
  }

}
