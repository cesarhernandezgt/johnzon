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
package org.apache.johnzon.mapper;

import static java.util.Collections.emptyList;

import org.apache.johnzon.mapper.internal.JsonPointerTracker;
import org.apache.johnzon.mapper.util.ArrayUtil;

import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MappingGeneratorImpl implements MappingGenerator {
    private final MapperConfig config;
    private final JsonGenerator generator;
    private final Mappings mappings;
    
    private final Boolean isDeduplicateObjects;
    private Map<Object, String> jsonPointers;


    MappingGeneratorImpl(MapperConfig config, JsonGenerator jsonGenerator, final Mappings mappings, Boolean isDeduplicateObjects) {
        this.config = config;
        this.generator = jsonGenerator;
        this.mappings = mappings;

        this.isDeduplicateObjects = isDeduplicateObjects;

        this.jsonPointers = isDeduplicateObjects ?  new HashMap<>() : Collections.emptyMap();
    }

    @Override
    public JsonGenerator getJsonGenerator() {
        return generator;
    }

    @Override
    public MappingGenerator writeObject(final String key, final Object object, final JsonGenerator generator) {
        if (object == null) {
            return this;
        } else if (object instanceof JsonValue) {
            generator.write(key, JsonValue.class.cast(object));
        } else {
            final Class<?> objectClass = object.getClass();
            try {
                if (Map.class.isInstance(object)) {
                    writeValue(Map.class, false, false, false, false, true, null, key, object,
                            null, emptyList(), isDeduplicateObjects ? new JsonPointerTracker(null, "/") : null, generator);
                } else if(writePrimitives(key, objectClass, object, generator)) {
                    // no-op
                } else if (objectClass.isEnum()) {
                    final Adapter adapter = config.findAdapter(objectClass);
                    final String adaptedValue = adapter.from(object).toString(); // we know it ends as String for enums
                    generator.write(key, adaptedValue);
                } else if (objectClass.isArray()) {
                    writeValue(Map.class, false, false, true, false, false, null, key, object,
                            null, emptyList(), isDeduplicateObjects ? new JsonPointerTracker(null, "/") : null, generator);
                } else if (Iterable.class.isInstance(object)) {
                    writeValue(Map.class, false, false, false, true, false, null, key, object,
                            null, emptyList(), isDeduplicateObjects ? new JsonPointerTracker(null, "/") : null, generator);
                } else {
                    final ObjectConverter.Writer objectConverter = config.findObjectConverterWriter(objectClass);
                    if (objectConverter != null) {
                        final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                                generator::writeStartObject, generator::writeEnd, null);
                        objectConverter.writeJson(object, dynamicMappingGenerator);
                        dynamicMappingGenerator.flushIfNeeded();
                    } else {
                        writeValue(objectClass, false, false, false, false, false, null, key, object,
                                null, emptyList(), isDeduplicateObjects ? new JsonPointerTracker(null, "/") : null, generator);
                    }
                }
            } catch (final InvocationTargetException | IllegalAccessException e) {
                throw new MapperException(e);
            }
        }
        return this;
    }

    @Override
    public MappingGenerator writeObject(final Object object, final JsonGenerator generator) {
        if (object == null) {
            return this;
        } else if (object instanceof JsonValue) {
            generator.write((JsonValue) object);
        } else {
            doWriteObject(object, generator, false, null, isDeduplicateObjects ? new JsonPointerTracker(null, "/") : null);
        }
        return this;
    }

    public void doWriteObject(Object object, JsonGenerator generator, boolean writeBody, final Collection<String> ignoredProperties, JsonPointerTracker jsonPointer) {

        try {
            if (object instanceof Map) {
                if (writeBody) {
                    generator.writeStartObject();
                }
                writeMapBody((Map<?, ?>) object, null);
                if (writeBody) {
                    generator.writeEnd();
                }
                return;
            }

            if(writePrimitives(object)) {
                return;
            }

            final Class<?> objectClass = object.getClass();
            if (objectClass.isEnum()) {
                final Adapter adapter = config.findAdapter(objectClass);
                final String adaptedValue = adapter.from(object).toString(); // we know it ends as String for enums
                generator.write(adaptedValue);
                return;
            }

            if (objectClass.isArray()) {
                final Adapter adapter = config.findAdapter(objectClass);
                writeArray(objectClass, adapter, null, object, ignoredProperties, jsonPointer);
                return;
            }

            if (object instanceof Iterable) {
                doWriteIterable((Iterable) object, ignoredProperties, jsonPointer);
                return;
            }

            ObjectConverter.Writer objectConverter = config.findObjectConverterWriter(objectClass);
            if (writeBody && objectConverter != null) {
                if (!writeBody) {
                    objectConverter.writeJson(object, this);
                } else {
                    final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                            generator::writeStartObject, generator::writeEnd, null);
                    objectConverter.writeJson(object, dynamicMappingGenerator);
                    dynamicMappingGenerator.flushIfNeeded();
                }
            } else {
                if (writeBody) {
                    generator.writeStartObject();
                }
                doWriteObjectBody(object, ignoredProperties, jsonPointer, generator);
                if (writeBody) {
                    generator.writeEnd();
                }
            }
        } catch (final InvocationTargetException | IllegalAccessException e) {
            throw new MapperException(e);
        }
    }

    private JsonGenerator writeMapBody(final Map<?, ?> object, final Adapter itemConverter) throws InvocationTargetException, IllegalAccessException {
        for (final Map.Entry<?, ?> entry : ((Map<?, ?>) object).entrySet()) {
            final Object value = entry.getValue();
            final Object key = entry.getKey();

            if (value == null) {
                if (config.isSkipNull()) {
                    continue;
                } else {
                    generator.writeNull(key == null ? "null" : key.toString());
                    continue;
                }
            }

            final Class<?> valueClass = value.getClass();
            writeValue(valueClass, true,
                    false, false, false, false, itemConverter,
                    key == null ? "null" : key.toString(), value, null, null, null,
                    generator);
        }
        return generator;
    }

    /**
     * @return {@code true} if it was a primitive, {@code false} if the value did not get handled
     */
    private boolean writePrimitives(final Object value) {
        boolean handled = false;
        if (value == null) {
            return true; // fake a write
        }

        final Class<?> type = value.getClass();
        if (type == String.class) {
            generator.write(value.toString());
            handled = true;
        } else if (type == long.class || type == Long.class) {
            generator.write(Long.class.cast(value).longValue());
            handled = true;
        } else if (isInt(type)) {
            generator.write(Number.class.cast(value).intValue());
            handled = true;
        } else if (isFloat(type)) {
            final double doubleValue = Number.class.cast(value).doubleValue();
            if (!Double.isNaN(doubleValue)) {
                generator.write(doubleValue);
            }
            handled = true;
        } else if (type == boolean.class || type == Boolean.class) {
            generator.write(Boolean.class.cast(value));
            return true;
        } else if (type == BigDecimal.class) {
            generator.write(BigDecimal.class.cast(value));
            handled = true;
        } else if (type == BigInteger.class) {
            generator.write(BigInteger.class.cast(value));
            handled = true;
        } else if (type == char.class || type == Character.class) {
            generator.write(Character.class.cast(value).toString());
            handled = true;
        }
        return handled;
    }

    private boolean writePrimitives(final String key, final Class<?> type, final Object value,
                                    final JsonGenerator generator) {
        boolean handled = false;
        if (type == String.class) {
            generator.write(key, value.toString());
            handled = true;
        } else if (JsonValue.class.isAssignableFrom(type)) {
            generator.write(key, JsonValue.class.cast(value));
            handled = true;
        } else if (type == long.class || type == Long.class) {
            generator.write(key, Long.class.cast(value).longValue());
            handled = true;
        } else if (isInt(type)) {
            generator.write(key, Number.class.cast(value).intValue());
            handled = true;
        } else if (isFloat(type)) {
            final double doubleValue = Number.class.cast(value).doubleValue();
            if (!Double.isNaN(doubleValue)) {
                generator.write(key, doubleValue);
            }
            handled = true;
        } else if (type == boolean.class || type == Boolean.class) {
            generator.write(key, Boolean.class.cast(value));
            handled = true;
        } else if (type == BigDecimal.class) {
            generator.write(key, BigDecimal.class.cast(value));
            handled = true;
        } else if (type == BigInteger.class) {
            generator.write(key, BigInteger.class.cast(value));
            handled = true;
        } else if (type == char.class || type == Character.class) {
            generator.write(key, Character.class.cast(value).toString());
            handled = true;
        }
        return handled;
    }


    private static boolean isInt(final Class<?> type) {
        return type == int.class || type == Integer.class
                || type == byte.class || type == Byte.class
                || type == short.class || type == Short.class;
    }

    private static boolean isFloat(final Class<?> type) {
        return type == double.class || type == Double.class
                || type == float.class || type == Float.class;
    }


    private void doWriteObjectBody(final Object object, final Collection<String> ignored,
                                   final JsonPointerTracker jsonPointer,final JsonGenerator generator)
            throws IllegalAccessException, InvocationTargetException {

        if (jsonPointer != null) {
            jsonPointers.put(object, jsonPointer.toString());
        }

        final Class<?> objectClass = object.getClass();
        final Mappings.ClassMapping classMapping = mappings.findOrCreateClassMapping(objectClass);
        if (classMapping == null) {
            throw new MapperException("No mapping for " + objectClass.getName());
        }

        if (classMapping.writer != null) {
            classMapping.writer.writeJson(object, this);
            return;
        }
        if (classMapping.adapter != null) {
            doWriteObjectBody(classMapping.adapter.to(object), ignored, jsonPointer, generator);
            return;
        }

        for (final Map.Entry<String, Mappings.Getter> getterEntry : classMapping.getters.entrySet()) {
            final Mappings.Getter getter = getterEntry.getValue();
            if (ignored != null && ignored.contains(getterEntry.getKey())) {
                continue;
            }
            if (getter.version >= 0 && config.getVersion() >= 0 && config.getVersion() < getter.version) {
                continue;
            }

            final Object value = getter.reader.read(object);
            if (JsonValue.class.isInstance(value)) {
                generator.write(getterEntry.getKey(), JsonValue.class.cast(value));
                continue;
            }

            if (value == null) {
                if (config.isSkipNull() && !getter.reader.isNillable()) {
                    continue;
                } else {
                    generator.writeNull(getterEntry.getKey());
                    continue;
                }
            }

            final Object val = getter.converter == null ? value : getter.converter.from(value);

            String valJsonPointer = jsonPointers.get(val);
            if (valJsonPointer != null) {
                // write the JsonPointer instead
                generator.write(getterEntry.getKey(), valJsonPointer);
            } else {
                writeValue(val.getClass(),
                        getter.dynamic,
                        getter.primitive,
                        getter.array,
                        getter.collection,
                        getter.map,
                        getter.itemConverter,
                        getterEntry.getKey(),
                        val,
                        getter.objectConverter,
                        getter.ignoreNested,
                        isDeduplicateObjects ? new JsonPointerTracker(jsonPointer, getterEntry.getKey()) : null,
                        generator);
            }
        }

        // @JohnzonAny doesn't respect comparator since it is a map and not purely in the model we append it after and
        // sorting is up to the user for this part (TreeMap if desired)
        if (classMapping.anyGetter != null) {
            final Map<String, Object> any = Map.class.cast(classMapping.anyGetter.reader.read(object));
            if (any != null) {
                writeMapBody(any, null);
            }
        }
    }

    //CHECKSTYLE:OFF
    private void writeValue(final Class<?> type, final boolean dynamic,
                            final boolean primitive, final boolean array,
                            final boolean collection, final boolean map,
                            final Adapter itemConverter,
                            final String key, final Object value,
                            final ObjectConverter.Writer objectConverter,
                            final Collection<String> ignoredProperties,
                            final JsonPointerTracker jsonPointer,
                            final JsonGenerator generator)
            throws InvocationTargetException, IllegalAccessException {
        //CHECKSTYLE:ON
        if (config.getSerializeValueFilter().shouldIgnore(key, value)) {
            return;
        }
        if (array || (dynamic && type.isArray())) {
            writeArray(type, itemConverter, key, value, ignoredProperties, jsonPointer);
        } else if (collection || (dynamic && Collection.class.isAssignableFrom(type))) {
            generator.writeStartArray(key);
            int i = 0;
            for (final Object o : Collection.class.cast(value)) {
                String valJsonPointer = jsonPointers.get(o);
                if (valJsonPointer != null) {
                    // write JsonPointer instead of the original object
                    writePrimitives(valJsonPointer);
                } else {
                    ObjectConverter.Writer objectConverterToUse = objectConverter;
                    if (o != null && objectConverterToUse == null) {
                        objectConverterToUse = config.findObjectConverterWriter(o.getClass());
                    }

                    if (objectConverterToUse != null) {
                        final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                                generator::writeStartObject, generator::writeEnd, null);
                        objectConverterToUse.writeJson(o, dynamicMappingGenerator);
                        dynamicMappingGenerator.flushIfNeeded();
                    } else {
                        writeItem(itemConverter != null ? itemConverter.from(o) : o, ignoredProperties,
                                isDeduplicateObjects ? new JsonPointerTracker(jsonPointer, i) : null);
                    }
                }
                i++;
            }
            generator.writeEnd();
        } else if (map || (dynamic && Map.class.isAssignableFrom(type))) {
            generator.writeStartObject(key);
            writeMapBody((Map<?, ?>) value, itemConverter);
            generator.writeEnd();
        } else if (primitive || (dynamic && Mappings.isPrimitive(type))) {
            if (objectConverter != null) {
                final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                        () -> this.generator.writeStartObject(key), this.generator::writeEnd, key);
                objectConverter.writeJson(value, dynamicMappingGenerator);
                dynamicMappingGenerator.flushIfNeeded();
            } else {
                writePrimitives(key, type, value, generator);
            }
        } else {
            if (objectConverter != null) {
                final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                        () -> this.generator.writeStartObject(key), this.generator::writeEnd, key);
                objectConverter.writeJson(value, dynamicMappingGenerator);
                dynamicMappingGenerator.flushIfNeeded();
                return;
            }

            final Adapter converter = config.findAdapter(type);
            if (converter != null) {
                final Object adapted = doConvertFrom(value, converter);
                if (writePrimitives(key, adapted.getClass(), adapted, generator)) {
                    return;
                }
                writeValue(String.class, true, true, false, false, false, null, key, adapted, null, ignoredProperties, jsonPointer, generator);
                return;
            } else {
                ObjectConverter.Writer objectConverterToUse = objectConverter;
                if (objectConverterToUse == null) {
                    objectConverterToUse = config.findObjectConverterWriter(type);
                }

                if (objectConverterToUse != null) {
                    final DynamicMappingGenerator dynamicMappingGenerator = new DynamicMappingGenerator(this,
                            () -> this.generator.writeStartObject(key), this.generator::writeEnd, key);
                    objectConverterToUse.writeJson(value, dynamicMappingGenerator);
                    dynamicMappingGenerator.flushIfNeeded();
                    return;
                }
            }
            if (writePrimitives(key, type, value, generator)) {
                return;
            }
            generator.writeStartObject(key);
            doWriteObjectBody(value, ignoredProperties, jsonPointer, generator);
            generator.writeEnd();
        }
    }

    /**
     * Write a JSON Array with a given Array Value, like byte[], int[], Person[] etc.
     * @param key either the attribute key or {@code null} if the array should be rendered without key
     */
    private void writeArray(Class<?> type, Adapter itemConverter, String key, Object arrayValue, Collection<String> ignoredProperties, JsonPointerTracker jsonPointer) {
        final int length = ArrayUtil.getArrayLength(arrayValue);
        if (length == 0 && config.isSkipEmptyArray()) {
            return;
        }

        if(config.isTreatByteArrayAsBase64() && (type == byte[].class /*|| type == Byte[].class*/)) {
            String base64EncodedByteArray = Base64.getEncoder().encodeToString((byte[]) arrayValue);
            if (key != null) {
                generator.write(key, base64EncodedByteArray);
            } else {
                generator.write(base64EncodedByteArray);
            }
            return;
        }
        if(config.isTreatByteArrayAsBase64URL() && (type == byte[].class /*|| type == Byte[].class*/)) {
            if (key != null) {
                generator.write(key, Base64.getUrlEncoder().encodeToString((byte[]) arrayValue));
            } else {
                generator.write(Base64.getUrlEncoder().encodeToString((byte[]) arrayValue));
            }
            return;
        }

        if (key != null) {
            generator.writeStartArray(key);
        } else {
            generator.writeStartArray();
        }

        // some specialised arrays to speed up conversion.
        // Needed since Array.get is rather slow :(
        if (type == byte[].class) {
            byte[] tArrayValue = (byte[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final byte o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == short[].class) {
            short[] tArrayValue = (short[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final short o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == int[].class) {
            int[] tArrayValue = (int[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final int o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == long[].class) {
            long[] tArrayValue = (long[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final long o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == float[].class) {
            float[] tArrayValue = (float[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final float o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == double[].class) {
            double[] tArrayValue = (double[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final double o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == char[].class) {
            char[] tArrayValue = (char[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final char o = tArrayValue[i];
                generator.write(String.valueOf(o));
            }
        } else if (type == boolean[].class) {
            boolean[] tArrayValue = (boolean[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final boolean o = tArrayValue[i];
                generator.write(o);
            }
        } else if (type == Byte[].class ||
                   type == Short[].class ||
                   type == Integer[].class ||
                   type == Long[].class ||
                   type == Float[].class ||
                   type == Double[].class ||
                   type == Character[].class ||
                   type == Boolean[].class) {
            // Wrapper types do not not need deduplication
            Object[] oArrayValue = (Object[]) arrayValue;
            for (int i = 0; i < length; i++) {
                final Object o = oArrayValue[i];
                writeItem(itemConverter != null ? itemConverter.from(o) : o, ignoredProperties, null);
            }
        } else {
            // must be object arrays
            for (int i = 0; i < length; i++) {
                Object[] oArrayValue = (Object[]) arrayValue;
                final Object o = oArrayValue[i];
                String valJsonPointer = jsonPointers.get(o);
                if (valJsonPointer != null) {
                    // write the JsonPointer as String natively
                    generator.write(valJsonPointer);
                } else if (o instanceof JsonValue) {
                    generator.write((JsonValue) o);
                } else {
                    writeItem(itemConverter != null ? itemConverter.from(o) : o, ignoredProperties, isDeduplicateObjects ? new JsonPointerTracker(jsonPointer, i) : null);
                }
            }
        }
        generator.writeEnd();
    }


    private void writeItem(final Object o, final Collection<String> ignoredProperties, JsonPointerTracker jsonPointer) {
        if (o == null) {
            generator.writeNull();
        } else if (!writePrimitives(o)) {
            if (Collection.class.isInstance(o)) {
                doWriteIterable(Collection.class.cast(o), ignoredProperties, jsonPointer);
            } else if (o.getClass().isArray()) {
                final int length = ArrayUtil.getArrayLength(o);
                if (length > 0 || !config.isSkipEmptyArray()) {
                    writeArray(o.getClass(), null, null, o, ignoredProperties, jsonPointer);
                }
            } else {
                String valJsonPointer = jsonPointers.get(o);
                if (valJsonPointer != null) {
                    // write the JsonPointer instead
                    generator.write(valJsonPointer);
                } else {
                    doWriteObject(o, generator, true, ignoredProperties, jsonPointer);
                }
            }
        }
    }

    private <T> void doWriteIterable(final Iterable<T> object, final Collection<String> ignoredProperties, JsonPointerTracker jsonPointer) {
        if (object == null) {
            generator.writeStartArray().writeEnd();
        } else {
            generator.writeStartArray();
            int i = 0;
            for (final T t : object) {
                if (JsonValue.class.isInstance(t)) {
                    generator.write(JsonValue.class.cast(t));
                } else {
                    if (t == null) {
                        generator.writeNull();
                    } else {
                        writeItem(t, ignoredProperties, isDeduplicateObjects ? new JsonPointerTracker(jsonPointer, i) : null);
                    }
                }
                i++;
            }
            generator.writeEnd();
        }
    }


    private <T> Object doConvertFrom(final T value, final Adapter<T, Object> converter) {
        if (converter == null) {
            throw new MapperException("can't convert " + value + " to String");
        }
        return converter.from(value);
    }

}
