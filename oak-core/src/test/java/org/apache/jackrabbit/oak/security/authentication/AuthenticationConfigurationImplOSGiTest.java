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
package org.apache.jackrabbit.oak.security.authentication;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.Constants;

import javax.jcr.SimpleCredentials;

import java.util.Map;
import java.util.Objects;

import static org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration.PARAM_APP_NAME;
import static org.junit.Assert.assertTrue;

public class AuthenticationConfigurationImplOSGiTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final AuthenticationConfigurationImpl authenticationConfiguration = new AuthenticationConfigurationImpl();

    private SimpleCredentials sc;

    @Override
    public void before() throws Exception {
        super.before();

        authenticationConfiguration.setSecurityProvider(getSecurityProvider());
    }

    @Test
    public void testGetParameters() {
        context.registerInjectActivateService(authenticationConfiguration, ConfigurationParameters.of(PARAM_APP_NAME, "name"));

        ConfigurationParameters expected = ConfigurationParameters.of(
                PARAM_APP_NAME, "name",
                Constants.SERVICE_PID, authenticationConfiguration.getClass().getName());
        assertTrue(areEqual(expected, authenticationConfiguration.getParameters()));
    }

    // helper methods
    private boolean areEqual(Map<String, Object> first, Map<String, Object> second) {
        if (first.size() != second.size()) {
            return false;
        }

        return first.entrySet().stream()
                .allMatch(e -> Objects.equals(e.getValue(), second.get(e.getKey())));
    }
}