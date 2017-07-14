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

package org.apache.hadoop.hive.cli;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.common.cli.CommonCliOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * OptionsProcessor.
 *
 */
public class OptionsProcessor {
  protected static final Logger l4j = LoggerFactory.getLogger(OptionsProcessor.class.getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;
  // hiveVariables保存的是用户使用命令行启动是通过define和hivevar定义的变量
  Map<String, String> hiveVariables = new HashMap<String, String>();

  @SuppressWarnings("static-access")
  public OptionsProcessor() {

    // -database database 进入Hive交互Shell时候指定数据库，默认进入default数据库
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("databasename")
        .withLongOpt("database")
        .withDescription("Specify the database to use")
        .create());

    // -e 'quoted-query-string' 命令行执行一段SQL语句, 如: bin/hive -e 'select *  from default.student'
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("quoted-query-string")
        .withDescription("SQL from command line")
        .create('e'));

    // -f <query-file> filename文件中保存HQL语句，执行其中的语句
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("filename")
        .withDescription("SQL from files")
        .create('f'));

    // -i <init-query-file> 进入Hive交互Shell时候先执行filename中的HQL语句, 即认为是一种初始化操作
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("filename")
        .withDescription("Initialization SQL file")
        .create('i'));

    // -hiveconf x=y 在命令行中设置Hive的运行时配置参数，优先级高于hive-site.xml,但低于Hive交互Shell中使用Set命令设置。
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Use value for given property")
        .create());

    // Substitution option -d, --define 定义一个变量值，这个变量可以在Hive交互Shell中引用，后面会介绍用法，比如：-d A=B
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("define")
        .withDescription("Variable subsitution to apply to hive commands. e.g. -d A=B or --define A=B")
        .create('d'));

    // Substitution option --hivevar 同—define
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("hivevar")
        .withDescription("Variable subsitution to apply to hive commands. e.g. --hivevar A=B")
        .create());

    // [-S|--silent] 静默模式，指定后不显示执行进度信息，最后只显示结果
    options.addOption(new Option("S", "silent", false, "Silent mode in interactive shell"));

    // [-v|--verbose] 冗余模式，额外打印出执行的HQL语句
    options.addOption(new Option("v", "verbose", false, "Verbose mode (echo executed SQL to the console)"));

    // [-H|--help] 显示帮助信息
    options.addOption(new Option("H", "help", false, "Print help information"));

  }

  public boolean process_stage1(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        // with HIVE-11304, hive.root.logger cannot have both logger name and log level.
        // if we still see it, split logger and level separately for hive.root.logger
        // and hive.log.level respectively
        if (propKey.equalsIgnoreCase("hive.root.logger")) {
          CommonCliOptions.splitAndSetLogger(propKey, confProps);
        } else {
          // 将hiveconf设置的key不是hive.root.logger的属性和值设置为系统参数
          System.setProperty(propKey, confProps.getProperty(propKey));
        }
      }

      // 通过define和hivevar定义的变量保存到hiveVariables中
      Properties hiveVars = commandLine.getOptionProperties("define");
      for (String propKey : hiveVars.stringPropertyNames()) {
        hiveVariables.put(propKey, hiveVars.getProperty(propKey));
      }

      Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
      for (String propKey : hiveVars2.stringPropertyNames()) {
        hiveVariables.put(propKey, hiveVars2.getProperty(propKey));
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      return false;
    }
    return true;
  }

  public boolean process_stage2(CliSessionState ss) {
    ss.getConf();

    if (commandLine.hasOption('H')) {
      printUsage();
      return false;
    }

    ss.setIsSilent(commandLine.hasOption('S'));

    ss.database = commandLine.getOptionValue("database");

    ss.execString = commandLine.getOptionValue('e');

    ss.fileName = commandLine.getOptionValue('f');

    ss.setIsVerbose(commandLine.hasOption('v'));

    String[] initFiles = commandLine.getOptionValues('i');
    if (null != initFiles) {
      ss.initFiles = Arrays.asList(initFiles);
    }

    if (ss.execString != null && ss.fileName != null) {
      System.err.println("The '-e' and '-f' options cannot be specified simultaneously");
      printUsage();
      return false;
    }

    if (commandLine.hasOption("hiveconf")) {
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      // 将hiveconf设置的属性和值设置赋值给SessionState的cmdProperties
      for (String propKey : confProps.stringPropertyNames()) {
        ss.cmdProperties.setProperty(propKey, confProps.getProperty(propKey));
      }
    }

    return true;
  }

  private void printUsage() {
    new HelpFormatter().printHelp("hive", options);
  }

  public Map<String, String> getHiveVariables() {
    return hiveVariables;
  }
}
