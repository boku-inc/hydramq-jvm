/*
 * The MIT License (MIT)
 *
 * Copyright © 2016-, Boku Inc., Jimmie Fulton
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit
 * persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package io.hydramq.internal.util;

import java.util.Collection;

/**
 * A utility class for performing runtime checks.  Primarily, this class is used for argument checking and state checking,
 * with violations throwing IllegalArgumentException and IllegalStateException, respectively.
 *
 * @author Jimmie Fulton
 * @author Mitch Wright
 */
public class Assert {

    /**
     * <p>Ensures that a method argument is not null.</p>
     *
     * <pre>
     * Assert.argumentNotNull(null, "argumentName")          // new IllegalArgumentException("'argumentName' cannot be null'");
     * Assert.argumentNotBlank(new Object(), "argumentName") // void
     * Assert.argumentNotBlank("", "argumentName")           // void
     * </pre>
     *
     * @param argumentValue argument to check
     * @param argumentName argument name, used in the generated error message
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentNotNull(final Object argumentValue, final String argumentName) {
        argumentIsTrue(argumentValue != null, "'" + argumentName + "' cannot be null");
    }

    /**
     * <p>Ensures that a CharSequence argument (such as String) is not whitespace, empty ("") or null.</p>
     *
     * <pre>
     * Assert.argumentNotBlank(null, "argumentName")   // new IllegalArgumentException("'argumentName' cannot be null or blank'");
     * Assert.argumentNotBlank("", "argumentName")     // new IllegalArgumentException("'argumentName' cannot be null or blank'");
     * Assert.argumentNotBlank(" ", "argumentName")    // new IllegalArgumentException("'argumentName' cannot be null or blank'");
     * Assert.argumentNotBlank("\n\t", "argumentName") // new IllegalArgumentException("'argumentName' cannot be null or blank'");
     * Assert.argumentNotBlank("bob", "argumentName")  // void
     * Assert.argumentNotBlank(" bob", "argumentName") // void
     * </pre>
     *
     * @param argumentValue  the CharSequence to check, may be null
     * @param argumentName argument name, used in the generated error message
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentNotBlank(final CharSequence argumentValue, final String argumentName) {
        argumentIsFalse(isBlank(argumentValue), notBlankMessage(argumentName));
    }

    /**
     * <p>Ensures that a CharSequence argument (such as String) is not empty ("") or null.</p>
     *
     * <pre>
     * Assert.argumentNotEmpty(null, "argumentName")   // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty("", "argumentName")     // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty(" ", "argumentName")    // void
     * Assert.argumentNotEmpty("\n", "argumentName")   // void
     * Assert.argumentNotEmpty("bob", "argumentName")  // void
     * Assert.argumentNotEmpty(" bob", "argumentName") // void
     * </pre>
     *
     * @param argumentValue  the CharSequence to check, may be null
     * @param argumentName argument name, used in the generated error message
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentNotEmpty(final CharSequence argumentValue, final String argumentName) {
        argumentIsFalse(isEmpty(argumentValue), notEmptyMessage(argumentName));
    }

    /**
     * <p>Ensures that a Collection argument is not empty or null.</p>
     *
     * <pre>
     * Assert.argumentNotEmpty(null, "argumentName")                        // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty(Lists.newArrayList(), "argumentName")        // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty(Sets.newHashSet("example"), "argumentName")  // void
     * </pre>
     *
     * @param argumentValue  the Collection to check, may be null
     * @param argumentName argument name, used in the generated error message
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentNotEmpty(final Collection<?> argumentValue, final String argumentName) {
        argumentIsFalse(isEmpty(argumentValue), notEmptyMessage(argumentName));
    }

    /**
     * <p>Ensures that an Object array argument is not empty or null.</p>
     *
     * <pre>
     * Assert.argumentNotEmpty(null, "argumentName")                      // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty(new String[] {}, "argumentName")           // new IllegalArgumentException("'argumentName' cannot be null or empty'");
     * Assert.argumentNotEmpty(new String[] {"example"}, "argumentName")  // void
     * </pre>
     *
     * @param argumentValue  the array to check, may be null
     * @param argumentName argument name, used in the generated error message
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentNotEmpty(final Object[] argumentValue, final String argumentName) {
        argumentIsFalse(isEmpty(argumentValue), notEmptyMessage(argumentName));
    }

    /**
     * <p>Ensures that an argument meets a condition that evaluates to true</p>
     *
     * Given: String value = "One";
     * <pre>
     * Assert.argumentIsTrue(value.equals("Two"), "'value' does not equal 'Two'") // new IllegalArgumentException("'value' does not equal 'Two'");
     * Assert.argumentIsTrue(value.equals("One"), "'value' does not equal 'One'") // void
     * </pre>
     *
     * @param argumentCondition boolean condition check on argument
     * @param conditionFailureMessage error message if condition evaluates to false
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentIsTrue(final boolean argumentCondition, final String conditionFailureMessage) {
        if (!argumentCondition) {
            throw new IllegalArgumentException(conditionFailureMessage);
        }
    }

    /**
     * <p>Ensures that an argument meets a condition that evaluates to false</p>
     *
     * Given: String value = "One";
     * <pre>
     * Assert.argumentIsFalse(value.equals("One"), "'value' cannot equal 'One'") // new IllegalArgumentException("'value' cannot equal 'One'");
     * Assert.argumentIsFalse(value.equals("Two"), "'value' cannot equal 'Two'") // void
     * </pre>
     *
     * @param argumentCondition boolean condition check on argument
     * @param conditionFailureMessage error message if condition evaluates to true
     * @throws java.lang.IllegalArgumentException
     */
    public static void argumentIsFalse(final boolean argumentCondition, final String conditionFailureMessage) {
        if (argumentCondition) {
            throw new IllegalArgumentException(conditionFailureMessage);
        }
    }

    /**
     * <p>Ensures that a given state meets a condition that evaluates to true</p>
     *
     * Given: context.isInitialized() == false;
     * <pre>
     * Assert.stateIsTrue(context.isInitialized(), "'context' requires initialization before use")    // new IllegalStateException("'context'' requires initialization before use");
     * Assert.stateIsTrue(context.isInitialized() == false, "'context' has already been initialized") // void
     * </pre>
     *
     * @param stateCondition boolean condition check on state
     * @param conditionFailureMessage error message if condition evaluates to false
     * @throws java.lang.IllegalStateException
     */
    public static void stateIsTrue(final boolean stateCondition, final String conditionFailureMessage) {
        if (!stateCondition) {
            throw new IllegalStateException(conditionFailureMessage);
        }
    }

    /**
     * <p>Ensures that a given state meets a condition that evaluates to false</p>
     *
     * Given: context.isInitialized() == true;
     * <pre>
     * Assert.stateIsFalse(context.isInitialized(), "'context' has already been initialized")                // new IllegalStateException("'context' has already been initialized");
     * Assert.stateIsFalse(context.isInitialized() == false, "'context' requires initialization before use") // void
     * </pre>
     *
     * @param stateCondition boolean condition check on state
     * @param conditionFailureMessage error message if condition evaluates to true
     * @throws java.lang.IllegalStateException
     */
    public static void stateIsFalse(final boolean stateCondition, final String conditionFailureMessage) {
        if (stateCondition) {
            throw new IllegalStateException(conditionFailureMessage);
        }
    }

    /**
     * Copied verbatim from commons-lang3's StringUtils.  Please use commons-lang for general "isBlank" checks.
     */
    private static boolean isBlank(final CharSequence cs) {
        final int strLen;
        if (cs == null || (strLen = cs.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isEmpty(final CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    private static boolean isEmpty(final Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    private static boolean isEmpty(final Object[] array) {
        return array == null || array.length == 0;
    }

    private static String notBlankMessage(final String argumentName) {
        return "'" + argumentName + "' cannot be null or blank";
    }

    private static String notEmptyMessage(final String argumentName) {
        return "'" + argumentName + "' cannot be null or empty";
    }
}
