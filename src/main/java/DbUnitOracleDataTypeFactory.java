/* Copyright 2009-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.dbunit.dataset.datatype.*;
import org.dbunit.ext.oracle.Oracle10DataTypeFactory;

import java.lang.reflect.Constructor;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * TODO
 *
 * @author Erich Eichinger
 * @since Apr 9, 2010
 */
public class DbUnitOracleDataTypeFactory extends Oracle10DataTypeFactory {

    public DataType createDataType(int sqlType, String sqlTypeName) throws DataTypeException {

        if (sqlType == oracle.jdbc.OracleTypes.INTERVALDS
                || sqlTypeName.startsWith("INTERVAL DAY")) {
            return new IntervalDSType();
        } else if (sqlType == oracle.jdbc.OracleTypes.INTERVALYM
                || sqlTypeName.startsWith("INTERVAL YEAR")) {
            return new IntervalYMType();
        }
        DataType res = super.createDataType(sqlType, sqlTypeName);
        if (res instanceof StringDataType) {
            return new StringDataTypeEx(sqlTypeName, sqlType);
        }
        return res;
    }

    public static class StringDataTypeEx extends StringDataType {
        public StringDataTypeEx(String name, int sqlType) {
            super(name, sqlType);
        }

        @Override
        public Object getSqlValue(int column, ResultSet resultSet) throws SQLException, TypeCastException {
            Object res = super.getSqlValue(column, resultSet);
            if (res instanceof String) {
                String strRes = (String) res;
                if ("\u0000".equals(strRes)) {
                    return "##NULL##";
                }
            }
            return res;
        }

        @Override
        public void setSqlValue(Object value, int column, PreparedStatement statement) throws SQLException, TypeCastException {
            if (value instanceof String) {
                String strValue = (String) value;
                if ("##NULL##".equals(strValue)) {
                    value = "\u0000";
                }
            }
            super.setSqlValue(value, column, statement);    //To change body of overridden methods use File | Settings | File Templates.
        }
    }
    
    public static class IntervalDSType extends AbstractDataType {

        public IntervalDSType() {
            super("intervalds", Types.OTHER, String.class, false);
        }

        public Object getSqlValue(int column, ResultSet resultSet) throws SQLException, TypeCastException {
            return resultSet.getString(column);
        }

        public void setSqlValue(Object interval, int column,
                                PreparedStatement statement) throws SQLException, TypeCastException {
            Object value = null;
            Class clazz = null;
            try {
                clazz = super.loadClass("oracle.sql.INTERVALDS", statement.getConnection() );
                Constructor ct = clazz.getConstructor(new Class[]{String.class});
                value = ct.newInstance(new Object[]{(String)interval});
            } catch (Exception e) {
                throw new TypeCastException("Failed creating instance of " + clazz + " from '" + interval + "'",  e);
            }
            statement.setObject(column, value);
        }

        public Object typeCast(Object arg0) throws TypeCastException {
            return arg0.toString();
        }
    }

    public static class IntervalYMType extends AbstractDataType {

        public IntervalYMType() {
            super("intervalym", Types.OTHER, String.class, false);
        }

        public Object getSqlValue(int column, ResultSet resultSet) throws SQLException, TypeCastException {
            return resultSet.getString(column);
        }

        public void setSqlValue(Object interval, int column,
                                PreparedStatement statement) throws SQLException, TypeCastException {
            Object value = null;
            Class clazz = null;
            try {
                clazz = super.loadClass("oracle.sql.INTERVALYM", statement.getConnection() );
                Constructor ct = clazz.getConstructor(new Class[]{String.class});
                value = ct.newInstance(new Object[]{(String)interval});
            } catch (Exception e) {
                throw new TypeCastException("Failed creating instance of " + clazz + " from '" + interval + "'",  e);
            }
            statement.setObject(column, value);
        }

        public Object typeCast(Object arg0) throws TypeCastException {
            return arg0.toString();
        }
    }
}
