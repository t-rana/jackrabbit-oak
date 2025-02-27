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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UserImporterImpersonationBestEffortTest extends UserImporterImpersonationIgnoreTest {

    @NotNull
    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_BESTEFFORT;
    }

    public void testUnknownImpersonators() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", "impersonator2"), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(Set.of("impersonator1", "impersonator2"), SetUtils.toSet(impersonators.getValue(Type.STRINGS)));
    }

    @Test
    public void testMixedImpersonators() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(Set.of("impersonator1", testUser.getPrincipal().getName()), SetUtils.toSet(impersonators.getValue(Type.STRINGS)));
    }
}