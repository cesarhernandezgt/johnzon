/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.johnzon.mapper.converter;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.johnzon.mapper.Mapper;
import org.apache.johnzon.mapper.MapperBuilder;
import org.junit.Test;

public class ClassConverterTest {
    @Test
    public void notRegisteredByDefault() { // no Class.forName is reachable from a document by default
        final Mapper mapper = new MapperBuilder().build();
        try {
            assertReadFails(mapper, Handler.class.getName(), "Missing a Converter for type class java.lang.Class");
        } finally {
            mapper.close();
        }
    }

    @Test
    public void rejectedWhenNotAllowed() { // registered but with the default - empty - allow lists
        final Mapper mapper = new MapperBuilder()
                .addConverter(Class.class, new ClassConverter())
                .build();
        try {
            assertReadFails(mapper, Handler.class.getName(), "is not an allowed class");
        } finally {
            mapper.close();
        }
    }

    @Test
    public void allowedByName() {
        final Mapper mapper = new MapperBuilder()
                .addConverter(Class.class, new ClassConverter(singletonList(Handler.class.getName()), emptyList()))
                .build();
        try {
            final Holder holder = mapper.readObject("{\"handler\":\"" + Handler.class.getName() + "\"}", Holder.class);
            assertEquals(Handler.class, holder.handler);
            assertReadFails(mapper, "java.lang.Runtime", "is not an allowed class");
        } finally {
            mapper.close();
        }
    }

    @Test
    public void allowedByPrefix() {
        final Mapper mapper = new MapperBuilder()
                .addConverter(Class.class, new ClassConverter(emptyList(), asList("org.apache.johnzon.mapper.converter.")))
                .build();
        try {
            final Holder holder = mapper.readObject("{\"handler\":\"" + Handler.class.getName() + "\"}", Holder.class);
            assertEquals(Handler.class, holder.handler);
            assertReadFails(mapper, "java.lang.Runtime", "is not an allowed class");
        } finally {
            mapper.close();
        }
    }

    @Test
    public void serializationDoesNotNeedAnAllowList() {
        final Mapper mapper = new MapperBuilder()
                .addConverter(Class.class, new ClassConverter())
                .build();
        try {
            final Holder holder = new Holder();
            holder.handler = Handler.class;
            assertEquals("{\"handler\":\"" + Handler.class.getName() + "\"}", mapper.writeObjectAsString(holder));
        } finally {
            mapper.close();
        }
    }

    private void assertReadFails(final Mapper mapper, final String className, final String expected) {
        try {
            mapper.readObject("{\"handler\":\"" + className + "\"}", Holder.class);
        } catch (final RuntimeException error) {
            assertCauseContains(error, expected);
            return;
        }
        fail("'" + className + "' should not have been deserialized");
    }

    private void assertCauseContains(final Throwable error, final String expected) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return;
            }
        }
        assertTrue("'" + expected + "' not found in " + error.getMessage(), false);
    }

    public static class Holder {
        public Class handler; // raw type: johnzon 1.0.x does not map a parameterized Class<?> property
    }

    public static class Handler {
    }
}
