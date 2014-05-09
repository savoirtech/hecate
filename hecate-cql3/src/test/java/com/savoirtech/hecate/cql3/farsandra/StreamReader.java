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

package com.savoirtech.hecate.cql3.farsandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.savoirtech.hecate.cql3.farsandra.LineHandler;

public class StreamReader implements Runnable {

    private InputStream is;
    private List<LineHandler> handlers;

    public StreamReader(InputStream is) {
        this.is = is;
        handlers = new ArrayList<LineHandler>();
    }

    public void addHandler(LineHandler handler) {
        handlers.add(handler);
    }

    @Override
    public void run() {
        String line = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
            while ((line = br.readLine()) != null) {
                for (LineHandler h : handlers) {
                    h.handleLine(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
