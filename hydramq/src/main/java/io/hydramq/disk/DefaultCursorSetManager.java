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

package io.hydramq.disk;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.hydramq.CursorSet;
import io.hydramq.CursorSetManager;
import io.hydramq.disk.flushing.FlushStrategies;
import io.hydramq.disk.flushing.FlushStrategy;
import io.hydramq.exceptions.HydraRuntimeException;
import io.hydramq.internal.util.Assert;

/**
 * @author jfulton
 */
public class DefaultCursorSetManager implements CursorSetManager {

    public static final String CURSOR_EXTENSION = ".cur";
    private ConcurrentHashMap<String, CursorSet> cursorSets = new ConcurrentHashMap<>();
    private Path baseDirectory;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final Supplier<FlushStrategy> flushStrategy;

    public DefaultCursorSetManager(Path baseDirectory, Supplier<FlushStrategy> flushStrategy) {
        this.baseDirectory = baseDirectory;
        this.flushStrategy = flushStrategy;
        try {
            Files.createDirectories(baseDirectory);
        } catch (IOException e) {
            throw new HydraRuntimeException("Error creating baseDirectory " + baseDirectory, e);
        }
        loadCursorSets();
    }

    public DefaultCursorSetManager(Path baseDirectory) {
        this(baseDirectory, FlushStrategies::standard);
    }

    @Override
    public boolean hasCursorSet(String cursorName) {
        return cursorSets.containsKey(normalizeKey(cursorName));
    }

    @Override
    public CursorSet getCursorSet(String cursorName) {
        return cursorSets.computeIfAbsent(normalizeKey(cursorName), key -> new DefaultCursorSet(baseDirectory.resolve(key + CURSOR_EXTENSION), flushStrategy.get()));
    }

    @Override
    public Set<String> getCursorSetNames() {
        Set<String> cursorSetNames = new HashSet<>();
        for (String key : cursorSets.keySet()) {
            cursorSetNames.add(key);
        }
        return cursorSetNames;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            for (CursorSet cursorSet : cursorSets.values()) {
                cursorSet.close();
            }
        }
    }

    private void loadCursorSets() {
        try {
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*" + CURSOR_EXTENSION);
            Files.walkFileTree(baseDirectory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (matcher.matches(file.getFileName())) {
                        String fileName = file.getFileName().toString();
                        getCursorSet(fileName.substring(0, fileName.length() - fileName.lastIndexOf(".") - 1));
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
                        throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

        } catch (IOException e) {
            throw new HydraRuntimeException("Error loading segments contained in " + baseDirectory.toString(), e);
        }
    }

    private String normalizeKey(String cursorName) {
        Assert.argumentNotBlank(cursorName, "cursorName");
        return cursorName.toLowerCase();
    }

    private void assertNotClosed() {
        if (closed.get()) {
            throw new IllegalStateException("Closed!");
        }
    }
}
