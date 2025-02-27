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
package org.apache.jackrabbit.oak.security.internal;

import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


class InternalSecurityProvider implements SecurityProvider, WhiteboardAware {

    private AuthenticationConfiguration authenticationConfiguration;

    private AuthorizationConfiguration authorizationConfiguration;

    private UserConfiguration userConfiguration;

    private PrivilegeConfiguration privilegeConfiguration;

    private PrincipalConfiguration principalConfiguration;

    private TokenConfiguration tokenConfiguration;

    private Whiteboard whiteboard;

    @NotNull
    @Override
    public ConfigurationParameters getParameters(@Nullable String name) {
        SecurityConfiguration securityConfiguration = getSecurityConfigurationByName(name);

        if (securityConfiguration == null) {
            return ConfigurationParameters.EMPTY;
        }

        return securityConfiguration.getParameters();
    }

    private @Nullable SecurityConfiguration getSecurityConfigurationByName(String name) {
        if (AuthenticationConfiguration.NAME.equals(name)) {
            return authenticationConfiguration;
        }

        if (AuthorizationConfiguration.NAME.equals(name)) {
            return authorizationConfiguration;
        }

        if (UserConfiguration.NAME.equals(name)) {
            return userConfiguration;
        }

        if (PrivilegeConfiguration.NAME.equals(name)) {
            return privilegeConfiguration;
        }

        if (PrincipalConfiguration.NAME.equals(name)) {
            return principalConfiguration;
        }

        if (TokenConfiguration.NAME.equals(name)) {
            return tokenConfiguration;
        }

        return null;
    }

    @NotNull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        return SetUtils.toSet(
                authenticationConfiguration,
                authorizationConfiguration,
                userConfiguration,
                privilegeConfiguration,
                principalConfiguration,
                tokenConfiguration
        );
    }

    @NotNull
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfiguration(@NotNull Class<T> configurationClass) {
        if (configurationClass == AuthenticationConfiguration.class) {
            return (T) authenticationConfiguration;
        }

        if (configurationClass == AuthorizationConfiguration.class) {
            return (T) authorizationConfiguration;
        }

        if (configurationClass == UserConfiguration.class) {
            return (T) userConfiguration;
        }

        if (configurationClass == PrivilegeConfiguration.class) {
            return (T) privilegeConfiguration;
        }

        if (configurationClass == PrincipalConfiguration.class) {
            return (T) principalConfiguration;
        }

        if (configurationClass == TokenConfiguration.class) {
            return (T) tokenConfiguration;
        }

        throw new IllegalArgumentException("Unsupported security configuration class " + configurationClass);
    }

    @Override
    public void setWhiteboard(@NotNull Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
    }

    @Override
    public @Nullable Whiteboard getWhiteboard() {
        return whiteboard;
    }

    public void setAuthenticationConfiguration(AuthenticationConfiguration authenticationConfiguration) {
        this.authenticationConfiguration = authenticationConfiguration;
    }

    public void setAuthorizationConfiguration(AuthorizationConfiguration authorizationConfiguration) {
        this.authorizationConfiguration = authorizationConfiguration;
    }

    public void setUserConfiguration(UserConfiguration userConfiguration) {
        this.userConfiguration = userConfiguration;
    }

    public void setPrivilegeConfiguration(PrivilegeConfiguration privilegeConfiguration) {
        this.privilegeConfiguration = privilegeConfiguration;
    }

    public void setPrincipalConfiguration(PrincipalConfiguration principalConfiguration) {
        this.principalConfiguration = principalConfiguration;
    }

    public void setTokenConfiguration(TokenConfiguration tokenConfiguration) {
        this.tokenConfiguration = tokenConfiguration;
    }

}
