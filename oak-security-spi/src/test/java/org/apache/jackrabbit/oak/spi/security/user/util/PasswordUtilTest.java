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
package org.apache.jackrabbit.oak.spi.security.user.util;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil.DEFAULT_ALGORITHM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class PasswordUtilTest {

    private List<String> plainPasswords;

    private static Map<String, String> hashedPasswords;

    @Before
    public void before() throws Exception {
        plainPasswords = List.of(
                "pw",
                "PassWord123",
                "_",
                "{invalidAlgo}",
                "{invalidAlgo}Password",
                "{SHA-256}",
                "pw{SHA-256}",
                "p{SHA-256}w",
                "");

        hashedPasswords = new HashMap<>();
        for (String pw : plainPasswords) {
            hashedPasswords.put(pw, PasswordUtil.buildPasswordHash(pw));
        }
    }

    @Test
    public void testBuildPasswordHash() throws Exception {
        for (String pw : plainPasswords) {
            String pwHash = PasswordUtil.buildPasswordHash(pw);
            assertNotEquals(pw, pwHash);
        }

        List<Integer[]> l = new ArrayList<>();
        l.add(new Integer[] {0, 1000});
        l.add(new Integer[] {1, 10});
        l.add(new Integer[] {8, 50});
        l.add(new Integer[] {10, 5});
        l.add(new Integer[] {-1, -1});
        for (Integer[] params : l) {
            for (String pw : plainPasswords) {
                int saltsize = params[0];
                int iterations = params[1];

                String pwHash = PasswordUtil.buildPasswordHash(pw, DEFAULT_ALGORITHM, saltsize, iterations);
                assertNotEquals(pw, pwHash);
            }
        }
    }

    @Test
    public void testBuildPasswordHashInvalidAlgorithm() throws Exception {
        List<String> invalidAlgorithms = new ArrayList<>();
        invalidAlgorithms.add("");
        invalidAlgorithms.add("+");
        invalidAlgorithms.add("invalid");

        for (String invalid : invalidAlgorithms) {
            try {
                PasswordUtil.buildPasswordHash("pw", invalid, PasswordUtil.DEFAULT_SALT_SIZE, PasswordUtil.DEFAULT_ITERATIONS);
                fail("Invalid algorithm " + invalid);
            } catch (NoSuchAlgorithmException e) {
                // success
            }
        }

    }

    @Test
    public void testBuildPasswordHashNoIterations() throws Exception {
        String hash = PasswordUtil.buildPasswordHash("pw", DEFAULT_ALGORITHM, PasswordUtil.DEFAULT_SALT_SIZE, 1);
        assertTrue(PasswordUtil.isSame(hash, "pw"));
    }

    @Test
    public void testBuildPasswordHashNoSalt() throws Exception {
        String hash = PasswordUtil.buildPasswordHash("pw", DEFAULT_ALGORITHM, 0, PasswordUtil.DEFAULT_ITERATIONS);
        assertTrue(PasswordUtil.isSame(hash, "pw"));
    }

    @Test
    public void testBuildPasswordHashNoSaltNoIterations() throws Exception {
        assumeFalse(DEFAULT_ALGORITHM.startsWith(PasswordUtil.PBKDF2_PREFIX));
        String jr2Hash = "{"+ DEFAULT_ALGORITHM+"}" + Text.digest(DEFAULT_ALGORITHM, "pw".getBytes("utf-8"));
        assertTrue(PasswordUtil.isSame(jr2Hash, "pw"));
    }

    @Test
    public void testBuiltPasswordHashNullAlgorithm() throws Exception {
        String hash = PasswordUtil.buildPasswordHash("pw", null, 0, PasswordUtil.DEFAULT_ITERATIONS);
        assertTrue(PasswordUtil.isSame(hash, "pw"));
        assertTrue(hash.startsWith("{" + DEFAULT_ALGORITHM + "}"));
    }

    @Test
    public void testBuildPasswordWithConfig() throws Exception {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConstants.PARAM_PASSWORD_SALT_SIZE, 13,
                UserConstants.PARAM_PASSWORD_HASH_ITERATIONS, 13);

        String hash = PasswordUtil.buildPasswordHash("pw", params);

        assertTrue(PasswordUtil.isSame(hash, "pw"));
    }

    @Test
    public void testIsPlainTextPassword() {
        for (String pw : plainPasswords) {
            assertTrue(pw + " should be plain text.", PasswordUtil.isPlainTextPassword(pw));
        }
    }

    @Test
    public void testIsPlainTextForNull() {
        assertTrue(PasswordUtil.isPlainTextPassword(null));
    }

    @Test
    public void testIsPlainTextForPwHash() {
        for (String pwHash : hashedPasswords.values()) {
            assertFalse(pwHash + " should not be plain text.", PasswordUtil.isPlainTextPassword(pwHash));
        }
    }

    @Test
    public void testIsSame() throws Exception {
        hashedPasswords.forEach((pw, pwHash) -> assertTrue("Not the same " + pw + ", " + pwHash, PasswordUtil.isSame(pwHash, pw)));

        String pw = "password";
        String pwHash = PasswordUtil.buildPasswordHash(pw, "SHA-1", 4, 50);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw.toCharArray()));

        pwHash = PasswordUtil.buildPasswordHash(pw, "md5", 0, 5);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw.toCharArray()));

        pwHash = PasswordUtil.buildPasswordHash(pw, "md5", -1, -1);
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw));
        assertTrue("Not the same '" + pw + "', " + pwHash, PasswordUtil.isSame(pwHash, pw.toCharArray()));
    }

    @Test
    public void testIsNotSame() {
        String previous = null;
        for (String pw : hashedPasswords.keySet()) {
            String pwHash = hashedPasswords.get(pw);
            assertFalse(pw, PasswordUtil.isSame(pw, pw));
            assertFalse(pwHash, PasswordUtil.isSame(pwHash, pwHash));
            if (previous != null) {
                assertFalse(previous, PasswordUtil.isSame(pwHash, previous));
            }
            previous = pw;
        }
    }

    @Test
    public void testIsSameNoSuchAlgorithmException() throws Exception {
        String hash = PasswordUtil.buildPasswordHash("pw");
        String invalid = "{invalidAlgorithm}" + hash.substring(hash.indexOf('}')+1);

        assertFalse(PasswordUtil.isSame(invalid, "pw"));
    }

    @Test
    public void testIsSameNullHash() {
        assertFalse(PasswordUtil.isSame(null, "pw"));
    }

    @Test
    public void testIsSameNullPw() throws Exception {
        assertFalse(PasswordUtil.isSame(PasswordUtil.buildPasswordHash("pw"), (String) null));
    }

    @Test
    public void testIsSameEmpty() throws Exception  {
        assertTrue(PasswordUtil.isSame(PasswordUtil.buildPasswordHash(""), ""));
    }

    @Test
    public void testIsSameEmptyHash() {
        assertFalse(PasswordUtil.isSame("", "pw"));
    }

    @Test
    public void testIsSameEmptyPw() throws Exception {
        assertFalse(PasswordUtil.isSame(PasswordUtil.buildPasswordHash("pw"), ""));
    }

    @Test
    public void testIsSameInvalidIterations() throws Exception {
        String hash = PasswordUtil.buildPasswordHash("pw", null, 5, 55);
        hash = hash.replace("-55-","-invalid-");

        assertFalse(PasswordUtil.isSame(hash, "pw"));
    }

    @Test
    public void testIsSameMisplacedAlgorithm() {
        List<String> broken = List.of("}{pw", "{pw", "{pw}");
        for (String hash : broken) {
            assertFalse(PasswordUtil.isSame(hash, "pw"));
        }
    }

    @Test
    public void testPBKDF2With() throws Exception {
        // https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html
        String algo = "PBKDF2WithHmacSHA512";
        // test vector from http://tools.ietf.org/html/rfc6070
        String pw = "pass\0word";
        int iterations = 4096;

        String hash = PasswordUtil.buildPasswordHash(pw, algo, 5, iterations);
        assertTrue(hash.startsWith("{" + algo + "}"));
        int cntOctets = hash.substring(hash.lastIndexOf('-') + 1).length() / 2;
        assertEquals(16, cntOctets);

        assertFalse(PasswordUtil.isPlainTextPassword(hash));
        assertTrue(PasswordUtil.isSame(hash, pw));
    }
}