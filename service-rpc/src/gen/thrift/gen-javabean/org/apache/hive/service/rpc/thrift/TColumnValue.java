/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.rpc.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
public class TColumnValue extends org.apache.thrift.TUnion<TColumnValue, TColumnValue._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TColumnValue");
  private static final org.apache.thrift.protocol.TField BOOL_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("boolVal", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField BYTE_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("byteVal", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField I16_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("i16Val", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField I32_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("i32Val", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField I64_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("i64Val", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField DOUBLE_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("doubleVal", org.apache.thrift.protocol.TType.STRUCT, (short)6);
  private static final org.apache.thrift.protocol.TField STRING_VAL_FIELD_DESC = new org.apache.thrift.protocol.TField("stringVal", org.apache.thrift.protocol.TType.STRUCT, (short)7);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    BOOL_VAL((short)1, "boolVal"),
    BYTE_VAL((short)2, "byteVal"),
    I16_VAL((short)3, "i16Val"),
    I32_VAL((short)4, "i32Val"),
    I64_VAL((short)5, "i64Val"),
    DOUBLE_VAL((short)6, "doubleVal"),
    STRING_VAL((short)7, "stringVal");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // BOOL_VAL
          return BOOL_VAL;
        case 2: // BYTE_VAL
          return BYTE_VAL;
        case 3: // I16_VAL
          return I16_VAL;
        case 4: // I32_VAL
          return I32_VAL;
        case 5: // I64_VAL
          return I64_VAL;
        case 6: // DOUBLE_VAL
          return DOUBLE_VAL;
        case 7: // STRING_VAL
          return STRING_VAL;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.BOOL_VAL, new org.apache.thrift.meta_data.FieldMetaData("boolVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TBoolValue.class)));
    tmpMap.put(_Fields.BYTE_VAL, new org.apache.thrift.meta_data.FieldMetaData("byteVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TByteValue.class)));
    tmpMap.put(_Fields.I16_VAL, new org.apache.thrift.meta_data.FieldMetaData("i16Val", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TI16Value.class)));
    tmpMap.put(_Fields.I32_VAL, new org.apache.thrift.meta_data.FieldMetaData("i32Val", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TI32Value.class)));
    tmpMap.put(_Fields.I64_VAL, new org.apache.thrift.meta_data.FieldMetaData("i64Val", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TI64Value.class)));
    tmpMap.put(_Fields.DOUBLE_VAL, new org.apache.thrift.meta_data.FieldMetaData("doubleVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TDoubleValue.class)));
    tmpMap.put(_Fields.STRING_VAL, new org.apache.thrift.meta_data.FieldMetaData("stringVal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TStringValue.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TColumnValue.class, metaDataMap);
  }

  public TColumnValue() {
    super();
  }

  public TColumnValue(_Fields setField, Object value) {
    super(setField, value);
  }

  public TColumnValue(TColumnValue other) {
    super(other);
  }
  public TColumnValue deepCopy() {
    return new TColumnValue(this);
  }

  public static TColumnValue boolVal(TBoolValue value) {
    TColumnValue x = new TColumnValue();
    x.setBoolVal(value);
    return x;
  }

  public static TColumnValue byteVal(TByteValue value) {
    TColumnValue x = new TColumnValue();
    x.setByteVal(value);
    return x;
  }

  public static TColumnValue i16Val(TI16Value value) {
    TColumnValue x = new TColumnValue();
    x.setI16Val(value);
    return x;
  }

  public static TColumnValue i32Val(TI32Value value) {
    TColumnValue x = new TColumnValue();
    x.setI32Val(value);
    return x;
  }

  public static TColumnValue i64Val(TI64Value value) {
    TColumnValue x = new TColumnValue();
    x.setI64Val(value);
    return x;
  }

  public static TColumnValue doubleVal(TDoubleValue value) {
    TColumnValue x = new TColumnValue();
    x.setDoubleVal(value);
    return x;
  }

  public static TColumnValue stringVal(TStringValue value) {
    TColumnValue x = new TColumnValue();
    x.setStringVal(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case BOOL_VAL:
        if (value instanceof TBoolValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TBoolValue for field 'boolVal', but got " + value.getClass().getSimpleName());
      case BYTE_VAL:
        if (value instanceof TByteValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TByteValue for field 'byteVal', but got " + value.getClass().getSimpleName());
      case I16_VAL:
        if (value instanceof TI16Value) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TI16Value for field 'i16Val', but got " + value.getClass().getSimpleName());
      case I32_VAL:
        if (value instanceof TI32Value) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TI32Value for field 'i32Val', but got " + value.getClass().getSimpleName());
      case I64_VAL:
        if (value instanceof TI64Value) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TI64Value for field 'i64Val', but got " + value.getClass().getSimpleName());
      case DOUBLE_VAL:
        if (value instanceof TDoubleValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TDoubleValue for field 'doubleVal', but got " + value.getClass().getSimpleName());
      case STRING_VAL:
        if (value instanceof TStringValue) {
          break;
        }
        throw new ClassCastException("Was expecting value of type TStringValue for field 'stringVal', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case BOOL_VAL:
          if (field.type == BOOL_VAL_FIELD_DESC.type) {
            TBoolValue boolVal;
            boolVal = new TBoolValue();
            boolVal.read(iprot);
            return boolVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case BYTE_VAL:
          if (field.type == BYTE_VAL_FIELD_DESC.type) {
            TByteValue byteVal;
            byteVal = new TByteValue();
            byteVal.read(iprot);
            return byteVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case I16_VAL:
          if (field.type == I16_VAL_FIELD_DESC.type) {
            TI16Value i16Val;
            i16Val = new TI16Value();
            i16Val.read(iprot);
            return i16Val;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case I32_VAL:
          if (field.type == I32_VAL_FIELD_DESC.type) {
            TI32Value i32Val;
            i32Val = new TI32Value();
            i32Val.read(iprot);
            return i32Val;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case I64_VAL:
          if (field.type == I64_VAL_FIELD_DESC.type) {
            TI64Value i64Val;
            i64Val = new TI64Value();
            i64Val.read(iprot);
            return i64Val;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DOUBLE_VAL:
          if (field.type == DOUBLE_VAL_FIELD_DESC.type) {
            TDoubleValue doubleVal;
            doubleVal = new TDoubleValue();
            doubleVal.read(iprot);
            return doubleVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case STRING_VAL:
          if (field.type == STRING_VAL_FIELD_DESC.type) {
            TStringValue stringVal;
            stringVal = new TStringValue();
            stringVal.read(iprot);
            return stringVal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case BOOL_VAL:
        TBoolValue boolVal = (TBoolValue)value_;
        boolVal.write(oprot);
        return;
      case BYTE_VAL:
        TByteValue byteVal = (TByteValue)value_;
        byteVal.write(oprot);
        return;
      case I16_VAL:
        TI16Value i16Val = (TI16Value)value_;
        i16Val.write(oprot);
        return;
      case I32_VAL:
        TI32Value i32Val = (TI32Value)value_;
        i32Val.write(oprot);
        return;
      case I64_VAL:
        TI64Value i64Val = (TI64Value)value_;
        i64Val.write(oprot);
        return;
      case DOUBLE_VAL:
        TDoubleValue doubleVal = (TDoubleValue)value_;
        doubleVal.write(oprot);
        return;
      case STRING_VAL:
        TStringValue stringVal = (TStringValue)value_;
        stringVal.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case BOOL_VAL:
          TBoolValue boolVal;
          boolVal = new TBoolValue();
          boolVal.read(iprot);
          return boolVal;
        case BYTE_VAL:
          TByteValue byteVal;
          byteVal = new TByteValue();
          byteVal.read(iprot);
          return byteVal;
        case I16_VAL:
          TI16Value i16Val;
          i16Val = new TI16Value();
          i16Val.read(iprot);
          return i16Val;
        case I32_VAL:
          TI32Value i32Val;
          i32Val = new TI32Value();
          i32Val.read(iprot);
          return i32Val;
        case I64_VAL:
          TI64Value i64Val;
          i64Val = new TI64Value();
          i64Val.read(iprot);
          return i64Val;
        case DOUBLE_VAL:
          TDoubleValue doubleVal;
          doubleVal = new TDoubleValue();
          doubleVal.read(iprot);
          return doubleVal;
        case STRING_VAL:
          TStringValue stringVal;
          stringVal = new TStringValue();
          stringVal.read(iprot);
          return stringVal;
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case BOOL_VAL:
        TBoolValue boolVal = (TBoolValue)value_;
        boolVal.write(oprot);
        return;
      case BYTE_VAL:
        TByteValue byteVal = (TByteValue)value_;
        byteVal.write(oprot);
        return;
      case I16_VAL:
        TI16Value i16Val = (TI16Value)value_;
        i16Val.write(oprot);
        return;
      case I32_VAL:
        TI32Value i32Val = (TI32Value)value_;
        i32Val.write(oprot);
        return;
      case I64_VAL:
        TI64Value i64Val = (TI64Value)value_;
        i64Val.write(oprot);
        return;
      case DOUBLE_VAL:
        TDoubleValue doubleVal = (TDoubleValue)value_;
        doubleVal.write(oprot);
        return;
      case STRING_VAL:
        TStringValue stringVal = (TStringValue)value_;
        stringVal.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case BOOL_VAL:
        return BOOL_VAL_FIELD_DESC;
      case BYTE_VAL:
        return BYTE_VAL_FIELD_DESC;
      case I16_VAL:
        return I16_VAL_FIELD_DESC;
      case I32_VAL:
        return I32_VAL_FIELD_DESC;
      case I64_VAL:
        return I64_VAL_FIELD_DESC;
      case DOUBLE_VAL:
        return DOUBLE_VAL_FIELD_DESC;
      case STRING_VAL:
        return STRING_VAL_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public TBoolValue getBoolVal() {
    if (getSetField() == _Fields.BOOL_VAL) {
      return (TBoolValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'boolVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setBoolVal(TBoolValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.BOOL_VAL;
    value_ = value;
  }

  public TByteValue getByteVal() {
    if (getSetField() == _Fields.BYTE_VAL) {
      return (TByteValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'byteVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setByteVal(TByteValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.BYTE_VAL;
    value_ = value;
  }

  public TI16Value getI16Val() {
    if (getSetField() == _Fields.I16_VAL) {
      return (TI16Value)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'i16Val' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setI16Val(TI16Value value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.I16_VAL;
    value_ = value;
  }

  public TI32Value getI32Val() {
    if (getSetField() == _Fields.I32_VAL) {
      return (TI32Value)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'i32Val' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setI32Val(TI32Value value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.I32_VAL;
    value_ = value;
  }

  public TI64Value getI64Val() {
    if (getSetField() == _Fields.I64_VAL) {
      return (TI64Value)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'i64Val' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setI64Val(TI64Value value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.I64_VAL;
    value_ = value;
  }

  public TDoubleValue getDoubleVal() {
    if (getSetField() == _Fields.DOUBLE_VAL) {
      return (TDoubleValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'doubleVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDoubleVal(TDoubleValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.DOUBLE_VAL;
    value_ = value;
  }

  public TStringValue getStringVal() {
    if (getSetField() == _Fields.STRING_VAL) {
      return (TStringValue)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'stringVal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setStringVal(TStringValue value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.STRING_VAL;
    value_ = value;
  }

  public boolean isSetBoolVal() {
    return setField_ == _Fields.BOOL_VAL;
  }


  public boolean isSetByteVal() {
    return setField_ == _Fields.BYTE_VAL;
  }


  public boolean isSetI16Val() {
    return setField_ == _Fields.I16_VAL;
  }


  public boolean isSetI32Val() {
    return setField_ == _Fields.I32_VAL;
  }


  public boolean isSetI64Val() {
    return setField_ == _Fields.I64_VAL;
  }


  public boolean isSetDoubleVal() {
    return setField_ == _Fields.DOUBLE_VAL;
  }


  public boolean isSetStringVal() {
    return setField_ == _Fields.STRING_VAL;
  }


  public boolean equals(Object other) {
    if (other instanceof TColumnValue) {
      return equals((TColumnValue)other);
    } else {
      return false;
    }
  }

  public boolean equals(TColumnValue other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(TColumnValue other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();
    list.add(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      list.add(setField.getThriftFieldId());
      Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        list.add(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        list.add(value);
      }
    }
    return list.hashCode();
  }
  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


}
