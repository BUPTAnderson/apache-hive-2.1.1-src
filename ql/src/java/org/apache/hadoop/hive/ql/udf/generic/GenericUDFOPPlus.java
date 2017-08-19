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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateColAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateColAddIntervalDayTimeScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateColAddIntervalYearMonthColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateColAddIntervalYearMonthScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateScalarAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DateScalarAddIntervalYearMonthColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColAddDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalColAddDecimalScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DecimalScalarAddDecimalColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColAddLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddDateScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddIntervalDayTimeScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddTimestampColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeColAddTimestampScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeScalarAddDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeScalarAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalDayTimeScalarAddTimestampColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddDateScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddIntervalYearMonthColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddIntervalYearMonthScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddTimestampColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthColAddTimestampScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthScalarAddDateColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthScalarAddIntervalYearMonthColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.IntervalYearMonthScalarAddTimestampColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColAddLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarAddDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarAddLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColAddIntervalDayTimeScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColAddIntervalYearMonthColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampColAddIntervalYearMonthScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampScalarAddIntervalDayTimeColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.TimestampScalarAddIntervalYearMonthColumn;

/**
 * The reason that we list evaluate methods with all numeric types is for both
 * better performance and type checking (so we know int + int is still an int
 * instead of a double); otherwise a single method that takes (Number a, Number
 * b) and use a.doubleValue() == b.doubleValue() is enough.
 *
 * The case of int + double will be handled by implicit type casting using
 * UDFRegistry.implicitConvertable method.
 * 加法对应的UDF
 */
@Description(name = "+", value = "a _FUNC_ b - Returns a+b")
@VectorizedExpressions({LongColAddLongColumn.class, LongColAddDoubleColumn.class,
  DoubleColAddLongColumn.class, DoubleColAddDoubleColumn.class, LongColAddLongScalar.class,
  LongColAddDoubleScalar.class, DoubleColAddLongScalar.class, DoubleColAddDoubleScalar.class,
  LongScalarAddLongColumn.class, LongScalarAddDoubleColumn.class, DoubleScalarAddLongColumn.class,
  DoubleScalarAddDoubleColumn.class, DecimalScalarAddDecimalColumn.class, DecimalColAddDecimalColumn.class,
  DecimalColAddDecimalScalar.class,
  IntervalYearMonthColAddIntervalYearMonthColumn.class,
  IntervalYearMonthColAddIntervalYearMonthScalar.class,
  IntervalYearMonthScalarAddIntervalYearMonthColumn.class,
  IntervalDayTimeColAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddIntervalDayTimeScalar.class,
  IntervalDayTimeScalarAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddTimestampColumn.class,
  IntervalDayTimeColAddTimestampScalar.class,
  IntervalDayTimeScalarAddTimestampColumn.class,
  TimestampColAddIntervalDayTimeColumn.class,
  TimestampColAddIntervalDayTimeScalar.class,
  TimestampScalarAddIntervalDayTimeColumn.class,
  DateColAddIntervalDayTimeColumn.class,
  DateColAddIntervalDayTimeScalar.class,
  DateScalarAddIntervalDayTimeColumn.class,
  IntervalDayTimeColAddDateColumn.class,
  IntervalDayTimeColAddDateScalar.class,
  IntervalDayTimeScalarAddDateColumn.class,
  IntervalYearMonthColAddDateColumn.class,
  IntervalYearMonthColAddDateScalar.class,
  IntervalYearMonthScalarAddDateColumn.class,
  IntervalYearMonthColAddTimestampColumn.class,
  IntervalYearMonthColAddTimestampScalar.class,
  IntervalYearMonthScalarAddTimestampColumn.class,
  DateColAddIntervalYearMonthColumn.class,
  DateScalarAddIntervalYearMonthColumn.class,
  DateColAddIntervalYearMonthScalar.class,
  TimestampColAddIntervalYearMonthColumn.class,
  TimestampScalarAddIntervalYearMonthColumn.class,
  TimestampColAddIntervalYearMonthScalar.class
})
public class GenericUDFOPPlus extends GenericUDFBaseArithmetic {

  public GenericUDFOPPlus() {
    super();
    this.opDisplayName = "+";
  }

  @Override
  protected GenericUDFBaseNumeric instantiateNumericUDF() {
    // 返回一个GenericUDFOPNumericPlus对象, 会调用该对象的evaluate方法
    return new GenericUDFOPNumericPlus();
  }

  @Override
  protected GenericUDF instantiateDTIUDF() {
    return new GenericUDFOPDTIPlus();
  }
}
