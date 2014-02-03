/*
 * Copyright 2014 Savoir Technologies
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.savoirtech.hecate.testing;

import java.util.Map;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.thrift.AuthenticationException;

public class TestAuthenticator implements IAuthenticator {

    public static final String SECURE_USER = "admin";
    public static final String SECURE_PASSWORD = "secret";

    @Override
    public AuthenticatedUser defaultUser() {
        return null;
    }

    @Override
    public AuthenticatedUser authenticate(Map<? extends CharSequence, ? extends CharSequence> credentials) throws AuthenticationException {

        String username = null;
        CharSequence user = credentials.get(IAuthenticator.USERNAME_KEY);
        if (null == user) {
            throw new AuthenticationException("Authentication request was missing the required key '" + IAuthenticator.USERNAME_KEY + "'");
        } else {
            username = user.toString();
        }

        String password = null;
        CharSequence pass = credentials.get(IAuthenticator.PASSWORD_KEY);
        if (null == pass) {
            throw new AuthenticationException("Authentication request was missing the required key '" + IAuthenticator.PASSWORD_KEY + "'");
        } else {
            password = pass.toString();
        }

        boolean authenticated = false;

        if (username.equals(SECURE_USER) && password.equals(SECURE_PASSWORD)) {
            authenticated = true;
        }

        if (!authenticated) {
            throw new AuthenticationException(authenticationErrorMessage(username));
        }

        return new AuthenticatedUser(username);
    }

    @Override
    public void validateConfiguration() throws ConfigurationException {
    }

    static String authenticationErrorMessage(String username) {
        return String.format("Given password could not be validated for user %s", username);
    }
}
