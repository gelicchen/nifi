/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.cdc.mysql.event;

import java.io.Serializable;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * A utility class to provide MySQL- / binlog-specific constants and methods for processing events and data
 */
public class MySQLCDCUtils {

    public static Object getWritableObject(Integer type, Serializable value) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else if (value instanceof Number) {
                return value;
            } else if (value instanceof Timestamp) {
                return parseTimestamp2Str((Timestamp) value);
            } else if (value instanceof Date) {
                return parseDate2Str((Date) value);
            }
        } else if (value instanceof Number) {
            return value;
        } else {
            if (type == Types.TIMESTAMP) {
                if (value instanceof Timestamp) {
                    return parseTimestamp2Str((Timestamp) value);
                }
                if (value instanceof Date) {
                    return parseDate2Str((Date) value);
                }
            }
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else {
                return value.toString();
            }
        }
        return null;
    }

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss",
            Locale.ENGLISH);

    private static String parseTimestamp2Str(Timestamp timestamp) {
        return timestamp.toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    /**
     * 添加对时间的解析,直接将时间转换成yyyy-MM-dd HH:mm:ss格式的字符字符串;
     * 由于mysql-binlog-connector-java中的AbstractRowsEventDataDeserializer.fallbackToGC 已经将时间读取成GMT时区,
     * 所以format的时候也必须指定为GMT
     */
    private static String parseDate2Str(Date date) {
        return date.toInstant().atZone(ZoneId.of("GMT")).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

}
