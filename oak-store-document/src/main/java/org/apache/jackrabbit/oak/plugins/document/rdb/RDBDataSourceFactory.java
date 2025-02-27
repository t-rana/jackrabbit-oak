/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.DataSource;

import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link DataSource}s based on a JDBC connection URL.
 */
public class RDBDataSourceFactory {

    static final Logger LOG = LoggerFactory.getLogger(RDBDataSourceFactory.class);

    public static DataSource forJdbcUrl(String url, String username, String passwd, String specifiedDriverName) {

        String driverName = specifiedDriverName;

        // load driver class when specified
        if (driverName != null && !driverName.isEmpty()) {
            LOG.info("trying to load specified driver {}", driverName);
        } else {
            // otherwise try to determine driver from JDBC URL
            driverName = RDBJDBCTools.driverForDBType(RDBJDBCTools.jdbctype(url));
            if (driverName != null && !driverName.isEmpty()) {
                LOG.info("trying to load defaulted driver {}", driverName);
            }
        }

        if (driverName != null && !driverName.isEmpty()) {
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException ex) {
                LOG.debug("driver " + driverName + " not loaded", ex);
                LOG.info("driver {} not loaded ({})", driverName, ex.getClass());
            }
        }

        try {
            LOG.info("Getting driver for {}", url);
            Driver d = DriverManager.getDriver(url);

            String classname = "org.apache.tomcat.jdbc.pool.DataSource";
            try {
                Class<?> dsclazz = Class.forName(classname);
                DataSource ds = (DataSource) dsclazz.getDeclaredConstructor().newInstance();
                dsclazz.getMethod("setDriverClassName", String.class).invoke(ds, d.getClass().getName());
                dsclazz.getMethod("setUsername", String.class).invoke(ds, username);
                dsclazz.getMethod("setPassword", String.class).invoke(ds, passwd);
                dsclazz.getMethod("setUrl", String.class).invoke(ds, url);
                String interceptors = SystemPropertySupplier
                        .create("org.apache.jackrabbit.oak.plugins.document.rdb.RDBDataSourceFactory.jdbcInterceptors",
                                "SlowQueryReport(threshold=10000);ConnectionState;StatementCache")
                        .loggingTo(LOG).get();
                if (!interceptors.isEmpty()) {
                    dsclazz.getMethod("setJdbcInterceptors", String.class).invoke(ds, interceptors);
                }
                return new CloseableDataSource(ds);
            } catch (Exception ex) {
                String message = "trying to create datasource " + classname;
                LOG.debug(message, ex);
                LOG.info(message + " (" + ex.getMessage() + ")");
                throw new DocumentStoreException(message, ex);
            }
        } catch (SQLException ex) {
            String message = "failed to to obtain driver for " + url;
            LOG.debug(message, ex);
            LOG.info(message + " (" + ex.getMessage() + ")");
            throw new DocumentStoreException(message, ex);
        }
    }

    public static DataSource forJdbcUrl(String url, String username, String passwd) {
        return forJdbcUrl(url, username, passwd, null);
    }

    /**
     * A {@link Closeable} {@link DataSource} based on a generic {@link Source}
     * .
     */
    private static class CloseableDataSource implements DataSource, Closeable {

        private DataSource ds;

        public CloseableDataSource(DataSource ds) {
            this.ds = ds;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return this.ds.getLogWriter();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return this.ds.getLoginTimeout();
        }

        @Override
        public void setLogWriter(PrintWriter pw) throws SQLException {
            this.ds.setLogWriter(pw);
        }

        @Override
        public void setLoginTimeout(int t) throws SQLException {
            this.ds.setLoginTimeout(t);
        }

        @Override
        public boolean isWrapperFor(Class<?> c) throws SQLException {
            return c.isInstance(this) || c.isInstance(this.ds) || this.ds.isWrapperFor(c);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T unwrap(Class<T> c) throws SQLException {
            return c.isInstance(this) ? (T) this : c.isInstance(this.ds) ? (T) this.ds : this.ds.unwrap(c);
        }

        @Override
        public void close() throws IOException {
            Class<?> dsclazz = ds.getClass();
            try {
                Method clmethod = dsclazz.getMethod("close");
                clmethod.invoke(ds);
            } catch (NoSuchMethodException e) {
                LOG.debug("Class " + dsclazz + " does not have close() method");
            } catch (IllegalArgumentException e) {
                LOG.debug("Class " + dsclazz + " does not have close() method");
            } catch (InvocationTargetException e) {
                throw new IOException("trying to close datasource", e);
            } catch (IllegalAccessException e) {
                throw new IOException("trying to close datasource", e);
            }
        }

        @Override
        public Connection getConnection() throws SQLException {
            return this.ds.getConnection();
        }

        @Override
        public Connection getConnection(String user, String passwd) throws SQLException {
            return this.ds.getConnection(user, passwd);
        }

        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public String toString() {
            return this.getClass().getName() + " wrapping a " + this.ds.toString();
        }
    }
}
