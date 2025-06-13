package org.umlg.sqlg.util;

import com.google.common.base.Strings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.jspecify.annotations.Nullable;

public class Preconditions {

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(stringValueOf(errorMessage));
        }
    }

    public static void checkState(boolean expression, @Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalStateException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(stringValueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean expression, String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        }
    }

    static String lenientFormat(@Nullable String template, @Nullable Object... args) {
        return Strings.lenientFormat(template, args);
    }

    static String stringValueOf(@Nullable Object o) {
        return String.valueOf(o);
    }

    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        } else {
            return reference;
        }
    }

    @CanIgnoreReturnValue
    public static <T> T checkNotNull(@Nullable T reference, @Nullable Object errorMessage) {
        if (reference == null) {
            throw new NullPointerException(stringValueOf(errorMessage));
        } else {
            return reference;
        }
    }

    @CanIgnoreReturnValue
    public static <T> T checkNotNull(@Nullable T reference, String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (reference == null) {
            throw new NullPointerException(lenientFormat(errorMessageTemplate, errorMessageArgs));
        } else {
            return reference;
        }
    }
}
