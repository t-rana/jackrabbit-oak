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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

/**
 * Extension of the default {@link org.apache.jackrabbit.oak.spi.security.authorization.cug.CugExclude}
 * implementation that allow to specify additional principal names to be excluded
 * from CUG evaluation.
 */
@Component(service = CugExclude.class, immediate = true)
@Designate(ocd = CugExcludeImpl.Configuration.class)
public class CugExcludeImpl extends CugExclude.Default {

    @ObjectClassDefinition(name = "Apache Jackrabbit Oak CUG Exclude List",
            description = "Exclude principal(s) from CUG evaluation. In addition to the " +
            "principals defined by the default CugExclude ('AdminPrincipal', 'SystemPrincipal', 'SystemUserPrincipal' classes), " +
            "this component allows to optionally configure additional principals by name.")
    @interface Configuration {
        @AttributeDefinition(
                name = "Principal Names",
                description = "Name(s) of additional principal(s) that are excluded from CUG evaluation.",
                cardinality = Integer.MAX_VALUE)
        String[] principalNames() default {};
    }

    private Set<String> principalNames = Collections.emptySet();

    @Override
    public boolean isExcluded(@NotNull Set<Principal> principals) {
        if (super.isExcluded(principals)) {
            return true;
        }
        if (!principalNames.isEmpty()) {
            for (Principal p : principals) {
                if (principalNames.contains(p.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Activate
    protected void activate(Map<String, Object> properties) {
        setPrincipalNames(properties);
    }

    @Modified
    protected void modified(Map<String, Object> properties) {
        setPrincipalNames(properties);
    }

    private void setPrincipalNames(@NotNull Map<String, Object> properties) {
        this.principalNames = Stream.of(PropertiesUtil.toStringArray(properties.get("principalNames"), new String[0])).filter(Objects::nonNull).collect(Collectors.toUnmodifiableSet());
    }
}
