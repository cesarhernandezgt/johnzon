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
package org.apache.johnzon.core;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.math.BigDecimal;
import java.util.Map;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;

public class JsonProviderImpl extends JsonProvider implements Serializable {
    private static final JsonProviderDelegate DELEGATE = new JsonProviderDelegate();
    //private int maxBigDecimalScale = Integer.getInteger("johnzon.max-big-decimal-scale", 1000);

    @Override
    public JsonParser createParser(final Reader reader) {
        return DELEGATE.createParser(reader);
    }

    @Override
    public JsonParser createParser(final InputStream inputStream) {
        return DELEGATE.createParser(inputStream);
    }

    @Override
    public JsonParserFactory createParserFactory(final Map<String, ?> stringMap) {
        return DELEGATE.createParserFactory(stringMap);
    }

    @Override
    public JsonGenerator createGenerator(final Writer writer) {
        return DELEGATE.createGenerator(writer);
    }

    @Override
    public JsonGenerator createGenerator(final OutputStream outputStream) {
        return DELEGATE.createGenerator(outputStream);
    }

    @Override
    public JsonGeneratorFactory createGeneratorFactory(final Map<String, ?> stringMap) {
        return DELEGATE.createGeneratorFactory(stringMap);
    }

    @Override
    public JsonReader createReader(final Reader reader) {
        return DELEGATE.createReader(reader);
    }

    @Override
    public JsonReader createReader(final InputStream inputStream) {
        return DELEGATE.createReader(inputStream);
    }

    @Override
    public JsonWriter createWriter(final Writer writer) {
        return DELEGATE.createWriter(writer);
    }

    @Override
    public JsonWriter createWriter(final OutputStream outputStream) {
        return DELEGATE.createWriter(outputStream);
    }

    @Override
    public JsonWriterFactory createWriterFactory(final Map<String, ?> stringMap) {
        return DELEGATE.createWriterFactory(stringMap);
    }

    @Override
    public JsonReaderFactory createReaderFactory(final Map<String, ?> stringMap) {
        return DELEGATE.createReaderFactory(stringMap);
    }

    @Override
    public JsonObjectBuilder createObjectBuilder() {
        return DELEGATE.createObjectBuilder();
    }

    @Override
    public JsonArrayBuilder createArrayBuilder() {
        return DELEGATE.createArrayBuilder();
    }

    @Override
    public JsonBuilderFactory createBuilderFactory(final Map<String, ?> stringMap) {
        return DELEGATE.createBuilderFactory(stringMap);
    }

    public int getMaxBigDecimalScale() {
        return DELEGATE.getMaxBigDecimalScale();
    }

    public void setMaxBigDecimalScale(final int maxBigDecimalScale) {
        DELEGATE.setMaxBigDecimalScale(maxBigDecimalScale);
    }
    
    static class JsonProviderDelegate extends JsonProvider {
        private final JsonReaderFactory readerFactory = new JsonReaderFactoryImpl(null, this);
        private final JsonParserFactory parserFactory = new JsonParserFactoryImpl(null, this);
        private final JsonGeneratorFactory generatorFactory = new JsonGeneratorFactoryImpl(null);
        private final JsonWriterFactory writerFactory = new JsonWriterFactoryImpl(null);
        private final JsonBuilderFactoryImpl builderFactory = new JsonBuilderFactoryImpl(null,this);

        private int maxBigDecimalScale = Integer.getInteger("johnzon.max-big-decimal-scale", 1_000);

        public int getMaxBigDecimalScale() {
            return maxBigDecimalScale;
        }

        public void setMaxBigDecimalScale(int value) {
            this.maxBigDecimalScale = value;
        }
        @Override
        public JsonParser createParser(final InputStream in) {
            return parserFactory.createParser(in);
        }

        @Override
        public JsonParser createParser(final Reader reader) {
            return parserFactory.createParser(reader);
        }

        @Override
        public JsonReader createReader(final InputStream in) {
            return readerFactory.createReader(in);
        }

        @Override
        public JsonReader createReader(final Reader reader) {
            return readerFactory.createReader(reader);
        }

        @Override
        public JsonParserFactory createParserFactory(final Map<String, ?> config) {
            return (config == null || config.isEmpty()) ? parserFactory : new JsonParserFactoryImpl(config,
                    this);
        }

        @Override
        public JsonReaderFactory createReaderFactory(final Map<String, ?> config) {
            return (config == null || config.isEmpty()) ? readerFactory : new JsonReaderFactoryImpl(config,
                    this);
        }

        @Override
        public JsonGenerator createGenerator(final Writer writer) {
            return generatorFactory.createGenerator(writer);
        }

        @Override
        public JsonGenerator createGenerator(final OutputStream out) {
            return generatorFactory.createGenerator(out);
        }

        @Override
        public JsonGeneratorFactory createGeneratorFactory(final Map<String, ?> config) {
            return (config == null || config.isEmpty()) ? generatorFactory : new JsonGeneratorFactoryImpl(config);
        }

        @Override
        public JsonWriter createWriter(final Writer writer) {
            return writerFactory.createWriter(writer);
        }

        @Override
        public JsonWriter createWriter(final OutputStream out) {
            return writerFactory.createWriter(out);
        }

        @Override
        public JsonWriterFactory createWriterFactory(final Map<String, ?> config) {
            return (config == null || config.isEmpty()) ? writerFactory : new JsonWriterFactoryImpl(config);
        }

        @Override
        public JsonObjectBuilder createObjectBuilder() {
            return builderFactory.createObjectBuilder();
        }

        @Override
        public JsonArrayBuilder createArrayBuilder() {
            return builderFactory.createArrayBuilder();
        }

        @Override
        public JsonBuilderFactory createBuilderFactory(final Map<String, ?> config) {
            return (config == null || config.isEmpty()) ? builderFactory : new JsonBuilderFactoryImpl(config,
                    this);
        }

        public void checkBigDecimalScale(final BigDecimal value) {
            // should be fine enough. Maybe we should externalize so users can pick something better if they need to
            // it becomes their responsibility to fix the limit and may expose them to a DoS attack
            final int limit = DELEGATE.getMaxBigDecimalScale();
            final int absScale = Math.abs(value.scale());

            if (absScale > limit) {
                throw new ArithmeticException(String.format(
                        "BigDecimal scale (%d) limit exceeds maximum allowed (%d)",
                        value.scale(), limit));
            }

        }
    }
    public void checkBigDecimalScale(final BigDecimal value) {
        DELEGATE.checkBigDecimalScale(value);
    }

}
