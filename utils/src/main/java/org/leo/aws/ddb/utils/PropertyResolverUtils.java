package org.leo.aws.ddb.utils;

import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyResolverUtils {
    private static final HashMap<String, String> PROPERTY_MAPPING = new HashMap<>();

    public static String getEnvironmentProperty(final String text, final Environment environment) {
        return getEnvironmentProperty(text, environment, PROPERTY_MAPPING::containsKey, PROPERTY_MAPPING::get, PROPERTY_MAPPING::put);
    }

    public static String getEnvironmentPropertyNoCaching(final String text, final Environment environment) {
        return getEnvironmentProperty(text, environment, a -> false, a -> null, (a, b) -> {});
    }

    public static String getEnvironmentProperty(final String text, final Environment environment, final Func1<String, Boolean> cacheCheckFunc, final Func1<String, String> cacheSupplierFunc, final Action2<String, String> cacheUpdateFunc) {

        // Check if the text is already been parsed
        if (cacheCheckFunc.call(text)) {

            return cacheSupplierFunc.call(text);

        }


        // If the text does not start with $, then no need to do pattern
        if (!text.startsWith("$")) {

            // Add to the mapping with key and value as text
            cacheUpdateFunc.call(text, text);

            // If no match, then return the text as it is
            return text;

        }

        // Create the pattern
        final Pattern pattern = Pattern.compile("\\Q${\\E(.+?)\\Q}\\E");

        // Create the matcher
        final Matcher matcher = pattern.matcher(text);

        // If the matching is there, then add it to the map and return the value
        if (matcher.find()) {

            // Store the value
            final String key = matcher.group(1);

            // Get the value
            final String value = environment.getProperty(key);

            // Store the value in the setting
            if (value != null) {

                // Store in the map
                cacheUpdateFunc.call(text, value);

                // return the value
                return value;

            }

        }

        // Add to the mapping with key and value as text
//        PROPERTY_MAPPING.put(text, text);
        cacheUpdateFunc.call(text, text);

        // If no match, then return the text as it is
        return text;
    }
}
