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

package io.hydramq;

import io.hydramq.MessageProperties.PropertyKey;

/**
 * @author jfulton
 */
public class PropertyKeys {

    public enum System implements PropertyKey {
        CORRELATION_ID("hydra.correlationId"),
        CLIENT_ID("hydra.clientId"),
        REPLY_TO("hydra.replyTo"),
        EXPIRATION("hydra.expiration"),
        TIMESTAMP("hydra.timestamp"),
        CONTENT_TYPE("hydra.contentType"),
        PRIORITY("hydra.priority"),
        ;

        private final String key;

        System(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return key;
        }
    }

    public enum RequestReply implements PropertyKey {
        CORRELATION_ID("request.correlationId"),
        CLIENT_ID("request.clientId"),
        REPLY_TO("request.replyTo"),
        ;

        private final String key;

        RequestReply(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return key;
        }
    }

    public enum Retry implements PropertyKey {
        EXPIRE_AFTER("retry.expireAfter"),
        PREVIOUS_RETRY_COUNT("retry.retryCount")
        ;

        private final String key;

        Retry(String key) {
            this.key = key;
        }

        @Override
        public String toString() {
            return key;
        }
    }
}
