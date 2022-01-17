package com.practice;

import com.practice.annotation.Column;
import com.practice.annotation.PrimaryKey;
import com.practice.annotation.Table;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConnectionFactory {

    private static Connection connection;

    private ConnectionFactory() throws SQLException, ClassNotFoundException {
        Class.forName ("org.h2.Driver");
        this.connection = DriverManager.getConnection ("jdbc:h2:/home/test/Downloads/h2/data", "sa","");

    }

    public static ConnectionFactory getConnection() throws SQLException, ClassNotFoundException {
        return new ConnectionFactory();
    }


    public void save(Object obj) throws SQLException, IllegalAccessException {
        String tableName = obj.getClass().getDeclaredAnnotation(Table.class).name();
        if (StringUtils.isBlank(tableName)) {
            tableName = obj.getClass().getSimpleName();
        }
        String primaryKey = "";
        Field pk = null;
        Field[] fields = obj.getClass().getDeclaredFields();
        List<Field> columnList = new ArrayList<>();
        StringJoiner joiner = new StringJoiner(",");
        for (Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKey = field.getDeclaredAnnotation(PrimaryKey.class).name();
                if(StringUtils.isBlank(primaryKey)) {
                    primaryKey = field.getName();
                }
                pk = field;
            } else {
                if (field.isAnnotationPresent(Column.class)) {
                    String column = field.getDeclaredAnnotation(Column.class).name();
                    if (StringUtils.isBlank(column)) {
                        column = field.getName();
                    }
                    columnList.add(field);
                    joiner.add(column);
                }
            }
        }
        int num = columnList.size()+1;

        String qString = IntStream.range(0, num).mapToObj(e -> "?").collect(Collectors.joining(",")).toString();

        String sql = "Insert into " + tableName + "(" + primaryKey +","+ joiner +")" + " values " + "("+ qString +")";

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setObject(1, pk.get(obj));
        int index = 2;
        for (int i = 0; i < columnList.size(); i++ ) {
            statement.setObject(index++, columnList.get(i).get(obj));
        }
        statement.executeUpdate();
    }

    public Object read (Class clazz, String pkValue) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException, NoSuchFieldException {
        Constructor<?> clzzConstructor = clazz.getConstructor();
        clzzConstructor.setAccessible(true);
        Object clzzObj = clzzConstructor.newInstance();
        String tableName = clzzObj.getClass().getDeclaredAnnotation(Table.class).name();
        if (StringUtils.isBlank(tableName)) {
            tableName = clazz.getSimpleName();
        }
        String primaryKey = "";

        Field[] fields = clazz.getDeclaredFields();
        List<String> columnList = new ArrayList<>();
        for (Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(PrimaryKey.class)) {
                primaryKey = field.getDeclaredAnnotation(PrimaryKey.class).name();
                if (StringUtils.isBlank(primaryKey)) {
                    primaryKey = field.getName();
                }
                columnList.add(primaryKey);
            } else {
                if (field.isAnnotationPresent(Column.class)) {
                    String column = field.getDeclaredAnnotation(Column.class).name();
                    if (StringUtils.isBlank(column)) {
                        column = field.getName();
                    }
                    columnList.add(column);
                }
            }
        }

        String sql = "select * from "+ tableName + " where " + primaryKey + "= '"+ pkValue + "';";
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()){
            for (String column : columnList) {
                clzzObj.getClass().getDeclaredField(column).set(clzzObj, resultSet.getObject(column));
            }
        }
        return clzzObj;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
