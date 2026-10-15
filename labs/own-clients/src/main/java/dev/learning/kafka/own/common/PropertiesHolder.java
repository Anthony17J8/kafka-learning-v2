package dev.learning.kafka.own.common;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public abstract class PropertiesHolder {

    protected abstract Set<String> ownProperties();

    protected abstract Map<String, Object> getProperties();

    protected abstract void handleCustoms(String[] keyVal);

    public void initProps(String[] args) {
        if (args == null) {
            return;
        }
        for (String arg : args) {
            String[] keyVal = arg.split("=", 2);
            if (keyVal.length != 2) {
                throw new IllegalArgumentException("Incorrect format for parameters: " + arg);
            }
            if (keyVal[0].isEmpty()) {
                throw new IllegalArgumentException("Key is empty");
            }
            if (keyVal[1].isEmpty()) {
                throw new IllegalArgumentException("Value is empty: " + keyVal[0] + "=");
            }
            if (ownProperties().contains(keyVal[0])) {
                handleCustoms(keyVal);
            } else {
                getProperties().put(keyVal[0], keyVal[1]);
            }
        }
    }

    protected static Long validateNumberAndGet(String key, String val) {
        try {
            return Long.parseLong(val);
        } catch (NumberFormatException exc) {
            throw new IllegalArgumentException("Incorrect value for key " + key + "=" + val);
        }
    }

    protected static void validateMoreThan0(String key, Long val) {
        if (val <= 0) {
            throw new IllegalArgumentException("Value for key must be positive(" + key + "=" + val + ")");
        }
    }

    protected static void validateNotNegative(String key, Long val) {
        if (val < 0) {
            throw new IllegalArgumentException("Value for key must be positive or 0(" + key + "=" + val + ")");
        }
    }

    protected static String validateNotEmpty(String key, String val) {
        if (val == null || val.trim().isEmpty()) {
            throw new IllegalArgumentException("Value for key is incorrect " + key + "=" + val);
        }
        return val;
    }

    protected static String validateFolder(String val) {
        Path folderPath = Path.of(val);
        boolean exists = Files.exists(folderPath);
        if (!exists) {
            throw new IllegalArgumentException(val + " doesn't exist");
        }
        boolean isDir = Files.isDirectory(folderPath);
        if (!isDir) {
            throw new IllegalArgumentException(val + " is not directory");
        }
        return val;
    }

    protected static String validateFile(String val) {
        Path fPath = Path.of(val);
        boolean exists = Files.exists(fPath);
        if (!exists) {
            throw new IllegalArgumentException(val + " doesn't exist");
        }
        boolean isFile = Files.isRegularFile(fPath);
        if (!isFile) {
            throw new IllegalArgumentException(val + " is not a file");
        }
        return val;
    }
}
