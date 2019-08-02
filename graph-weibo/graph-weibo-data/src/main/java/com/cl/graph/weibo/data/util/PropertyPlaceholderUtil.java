package com.cl.graph.weibo.data.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessControlException;
import java.util.*;

/**
 * Copyright © 2018 eSunny Info. Developer Stu. All rights reserved.
 * <p>
 * code is far away from bug with the animal protecting
 * <p>
 * ┏┓　　　┏┓
 * ┏┛┻━━━┛┻┓
 * ┃　　　　　　　┃
 * ┃　　　━　　　┃
 * ┃　┳┛　┗┳　┃
 * ┃　　　　　　　┃
 * ┃　　　┻　　　┃
 * ┃　　　　　　　┃
 * ┗━┓　　　┏━┛
 * 　　┃　　　┃神兽保佑
 * 　　┃　　　┃代码无BUG！
 * 　　┃　　　┗━━━┓
 * 　　┃　　　　　　　┣┓
 * 　　┃　　　　　　　┏┛
 * 　　┗┓┓┏━┳┓┏┛
 * 　　　┃┫┫　┃┫┫
 * 　　　┗┻┛　┗┻┛
 *
 * @author zpx
 * Build File @date: 2019/7/2 16:45
 * @version 1.0
 * @description
 */
@SuppressWarnings("all")
public class PropertyPlaceholderUtil {

    private static final Logger logger = LoggerFactory.getLogger(PropertyPlaceholderUtil.class);
    private static final Map<String, String> WELL_KNOWN_SIMPLE_PREFIXES = new HashMap<>(4);
    private PropertySourceMap propertySourceMap;

    static {
        WELL_KNOWN_SIMPLE_PREFIXES.put("}", "{");
        WELL_KNOWN_SIMPLE_PREFIXES.put("]", "[");
        WELL_KNOWN_SIMPLE_PREFIXES.put(")", "(");
    }


    private final String placeholderPrefix;

    private final String placeholderSuffix;

    private final String simplePrefix;

    private final String valueSeparator;

    private final boolean ignoreUnresolvablePlaceholders;


    public PropertyPlaceholderUtil(String placeholderPrefix, String placeholderSuffix) {
        this(placeholderPrefix, placeholderSuffix, null, true);
    }

    private PropertyPlaceholderUtil createPlaceholderUtil(boolean ignoreUnresolvablePlaceholders) {
        return new PropertyPlaceholderUtil(this.placeholderPrefix, this.placeholderSuffix,
                this.valueSeparator, ignoreUnresolvablePlaceholders);
    }


    public PropertyPlaceholderUtil(String placeholderPrefix, String placeholderSuffix,
                                   String valueSeparator, boolean ignoreUnresolvablePlaceholders) {

        this.placeholderPrefix = placeholderPrefix;
        this.placeholderSuffix = placeholderSuffix;
        String simplePrefixForSuffix = WELL_KNOWN_SIMPLE_PREFIXES.get(this.placeholderSuffix);
        if (simplePrefixForSuffix != null && this.placeholderPrefix.endsWith(simplePrefixForSuffix)) {
            this.simplePrefix = simplePrefixForSuffix;
        } else {
            this.simplePrefix = this.placeholderPrefix;
        }
        this.valueSeparator = valueSeparator;
        this.ignoreUnresolvablePlaceholders = ignoreUnresolvablePlaceholders;
    }


    /**
     * Replaces all placeholders of format {@code ${name}} with the corresponding
     * property from the supplied {@link Properties}.
     *
     * @param value the value containing the placeholders to be replaced
     * @return the supplied value with placeholders replaced inline
     */
    public String replacePlaceholders(String value) {
        return parseStringValue(value, null);
    }

    protected String parseStringValue(
            String value, Set<String> visitedPlaceholders) {

        int startIndex = value.indexOf(this.placeholderPrefix);
        if (startIndex == -1) {
            return value;
        }

        StringBuilder result = new StringBuilder(value);
        while (startIndex != -1) {
            int endIndex = findPlaceholderEndIndex(result, startIndex);
            if (endIndex != -1) {
                String placeholder = result.substring(startIndex + this.placeholderPrefix.length(), endIndex);
                String originalPlaceholder = placeholder;
                if (visitedPlaceholders == null) {
                    visitedPlaceholders = new HashSet<>(4);
                }
                if (!visitedPlaceholders.add(originalPlaceholder)) {
                    throw new IllegalArgumentException(
                            "Circular placeholder reference '" + originalPlaceholder + "' in property definitions");
                }
                // Recursive invocation, parsing placeholders contained in the placeholder key.
                placeholder = parseStringValue(placeholder, visitedPlaceholders);
                // Now obtain the value for the fully resolved key...
//                String propVal = placeholderResolver.resolvePlaceholder(placeholder);
                Object obj = resolvePlaceholder(placeholder);
                String propVal = obj == null ? null : obj.toString();
                if (propVal == null && this.valueSeparator != null) {
                    int separatorIndex = placeholder.indexOf(this.valueSeparator);
                    if (separatorIndex != -1) {
                        String actualPlaceholder = placeholder.substring(0, separatorIndex);
                        String defaultValue = placeholder.substring(separatorIndex + this.valueSeparator.length());
                        Object obj1 = resolvePlaceholder(actualPlaceholder);
                        propVal = obj1 == null ? null : obj1.toString();
                        if (propVal == null) {
                            if(defaultValue.contains(this.valueSeparator)){
                                return parseStringValue(this.placeholderPrefix + defaultValue + this.placeholderSuffix,null);
                            }
                            propVal = defaultValue;
                        }
                    }
                }
                if (propVal != null) {
                    // Recursive invocation, parsing placeholders contained in the
                    // previously resolved placeholder value.
                    propVal = parseStringValue(propVal, visitedPlaceholders);
                    result.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Resolved placeholder '" + placeholder + "'");
                    }
                    startIndex = result.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {
                    // Proceed with unprocessed value.
                    startIndex = result.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new IllegalArgumentException("Could not resolve placeholder '" +
                            placeholder + "'" + " in value \"" + value + "\"");
                }
                visitedPlaceholders.remove(originalPlaceholder);
            } else {
                startIndex = -1;
            }
        }
        return result.toString();
    }

    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        int index = startIndex + this.placeholderPrefix.length();
        int withinNestedPlaceholder = 0;
        while (index < buf.length()) {
            if (substringMatch(buf, index, this.placeholderSuffix)) {

                if (withinNestedPlaceholder > 0) {
                    withinNestedPlaceholder--;
                    index = index + this.placeholderSuffix.length();
                } else {
                    return index;
                }
            } else if (substringMatch(buf, index, this.simplePrefix)) {
                withinNestedPlaceholder++;
                index = index + this.simplePrefix.length();
            } else {
                index++;
            }
        }
        return -1;
    }


    /**
     * Strategy interface used to resolve replacement values for placeholders contained in Strings.
     */
    @FunctionalInterface
    public interface PlaceholderResolver {

        /**
         * Resolve the supplied placeholder name to the replacement value.
         *
         * @param placeholderName the name of the placeholder to resolve
         * @return the replacement value, or {@code null} if no replacement is to be made
         */
        String resolvePlaceholder(String placeholderName);
    }

    private static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        if (index + substring.length() > str.length()) {
            return false;
        }
        for (int i = 0; i < substring.length(); i++) {
            if (str.charAt(index + i) != substring.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private Object resolvePlaceholder(String key) {
        if (this.propertySourceMap == null) {
            this.propertySourceMap = new PropertySourceMap();
        }
        return this.propertySourceMap.get(key);
    }

    private static class PropertySourceMap {
        private List<Map<String, Object>> list;

        PropertySourceMap() {
            list = new LinkedList<>();
            list.add(getSystemEnvironment());
            list.add(getSystemProperties());
        }

        public Object get(String key) {
            assert list != null;
            for (Map<String, Object> map : list) {
                Object o = map.get(key);
                if (o != null) {
                    return o;
                }
            }
            return null;
        }

        private Map<String, Object> getSystemProperties() {
            try {
                return (Map) System.getProperties();
            } catch (AccessControlException ex) {
                return (Map) new AbstractReadOnlySystemAttributesMap() {
                    @Override
                    protected String getSystemAttribute(String attributeName) {
                        try {
                            return System.getProperty(attributeName);
                        } catch (AccessControlException ex) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Caught AccessControlException when accessing system property '" +
                                        attributeName + "'; its value will be returned [null]. Reason: " + ex.getMessage());
                            }
                            return null;
                        }
                    }
                };
            }
        }

        private Map<String, Object> getSystemEnvironment() {
            try {
                return (Map) System.getenv();
            } catch (AccessControlException ex) {
                return (Map) new AbstractReadOnlySystemAttributesMap() {
                    @Override
                    protected String getSystemAttribute(String attributeName) {
                        try {
                            return System.getenv(attributeName);
                        } catch (AccessControlException ex) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Caught AccessControlException when accessing system environment variable '" +
                                        attributeName + "'; its value will be returned [null]. Reason: " + ex.getMessage());
                            }
                            return null;
                        }
                    }
                };
            }
        }
    }

    abstract static class AbstractReadOnlySystemAttributesMap implements Map<String, String> {

        @Override
        public boolean containsKey(Object key) {
            return (get(key) != null);
        }

        /**
         * Returns the value to which the specified key is mapped, or {@code null} if this map
         * contains no mapping for the key.
         *
         * @param key the name of the system attribute to retrieve
         * @throws IllegalArgumentException if given key is non-String
         */
        @Override
        public String get(Object key) {
            if (!(key instanceof String)) {
                throw new IllegalArgumentException(
                        "Type of key [" + key.getClass().getName() + "] must be java.lang.String");
            }
            return getSystemAttribute((String) key);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        /**
         * Template method that returns the underlying system attribute.
         * <p>Implementations typically call {@link System#getProperty(String)} or {@link System#getenv(String)} here.
         */
        protected abstract String getSystemAttribute(String attributeName);


        // Unsupported

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String put(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            return Collections.emptySet();
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> map) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<String> values() {
            return Collections.emptySet();
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            return Collections.emptySet();
        }

    }

}
