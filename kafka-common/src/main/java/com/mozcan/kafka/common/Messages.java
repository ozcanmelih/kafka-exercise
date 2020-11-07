package com.mozcan.kafka.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Messages {

    private static final Logger logger = LoggerFactory.getLogger(Messages.class);

    private static final Properties props;

    static {
        props = new Properties();
        final InputStream stream = Messages.class.getResourceAsStream("/application.properties");
        if (stream == null) {
            throw new RuntimeException("No properties!!!");
        }
        try {
            props.load(stream);
        } catch (IOException e) {
            logger.error("application.properties cannot be loaded.", e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                logger.error("Input Stream cannot be closed.", e);
            }
        }
    }

    public static String getAsString(String key) {
        return (String) props.get(key);
    }

    public static Integer getAsInteger(String key) {
        return Integer.valueOf((String) props.get(key));
    }
}
