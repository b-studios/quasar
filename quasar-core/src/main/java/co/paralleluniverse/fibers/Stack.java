/*
 * Quasar: lightweight threads and actors for the JVM.
 * Copyright (c) 2013-2015, Parallel Universe Software Co. All rights reserved.
 *
 * This program and the accompanying materials are dual-licensed under
 * either the terms of the Eclipse Public License v1.0 as published by
 * the Eclipse Foundation
 *
 *   or (per the licensee's choosing)
 *
 * under the terms of the GNU Lesser General Public License version 3.0
 * as published by the Free Software Foundation.
 */
package co.paralleluniverse.fibers;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Internal Class - DO NOT USE! (Public so that instrumented code can access it)
 *
 * ANY CHANGE IN THIS CLASS NEEDS TO BE SYNCHRONIZED WITH {@link co.paralleluniverse.fibers.instrument.InstrumentMethod}
 *
 * This is meant as a drop in replacement for the old Stack-class with
 * worse performance but easier to maintain.
 *
 * The stackpointer always points at the currently active frame.
 * Entering a frame does not push a frame!
 *
 * Invariant: not used slots in the frames array are filled with `null`
 *            to determine the top of the stack.
 *
 * Invariant: Frames are not reused! That is, once a frame is pushed and
 *            the fields are set, they shouldn't change anymore.
 */
public final class Stack implements Serializable {

    //<editor-fold defaultstate="collapsed" desc="Constants">
    private static final int EMPTY = -1;
    private static final int INITIAL_METHOD_STACK_DEPTH = 16;
    private static final long serialVersionUID = 12786283751253L;
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Fields">
    int sp = EMPTY;
    Frame[] frames;
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Constructors">

    public Stack(int stackSize) {
        this(null, stackSize);
    }

    public Stack(Object context, int stackSize) {
        if (stackSize <= 0)
            throw new IllegalArgumentException("stackSize: " + stackSize);

        this.frames = new Frame[stackSize];
        this.context = context;
        this.sp = EMPTY;
    }

    public Stack(Object context, int sp, Frame[] frames) {
        this.context = context;
        this.frames = frames;
        this.sp = sp;
    }

    // copying constructor, copies from given s into this
    public Stack(Object context, Stack s) {
        this.context = context;
        this.frames = Arrays.copyOf(s.frames, s.frames.length);
        this.sp = EMPTY;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="DelimCC Operations">
    public final Marker getMarker() {
        return new Marker(sp);
    }

    public final void resumeAt(Marker frame) {
        sp = frame.pointer;
    }

    /**
     * Splits the stack at position frame, copies everything into
     * a stack segment and zeros out the next frame record.
     *
     * @param frame
     * @return the segment above frame
     */
    public final Stack popSegmentAt(Marker frame) {
        assert (frame.pointer > sp) :
            "Can't copy something above stack pointer: " + sp + ". Marker is " + frame.pointer;

        Stack seg = new Stack(context,
            sp - frame.pointer,
            Arrays.copyOfRange(frames, frame.pointer, sp + 1));

        sp = frame.pointer - 1;
        clearAbove(sp);
        return seg;
    }

    public final void pushSegment(Stack s) {
        // (1) grow stack to fit segment
        int newLength = sp + 1 + s.frames.length;
        growStack(newLength);

        // (2) copy segment
        System.arraycopy(s.frames, 0, frames, sp + 1, s.frames.length);

        // (3) set stack pointer to last frame on stack
        sp = sp + s.sp + 1;

        // (4) for safety, clear everything above sp
        clearAbove(sp);
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Quasar Stack Operations">
    Frame currentFrame() {
        assert (sp != EMPTY) : "Can't get currentFrame on empty stack.";
        return frames[sp];
    }

    /**
     * Called at the beginning of a method to return the
     * jump-label of the next entry point or 0, if
     * the method is being entered for the first time.
     *
     * Also moves the stack pointer!
     *
     * @return the entry point of this method
     */
    public final int nextMethodEntry() {
        sp += 1;

        // make sure the slot for next frame actually exists
        growStack(sp);

        Frame next = frames[sp];
        if (next == null) {
            return 0;
        } else {
            return next.entry;
        }
    }

    /**
     * Called before another, effectful method is called.
     * Reserves enough space on the stack to stores the state of the
     * current method, before entering the other method.
     *
     * Doesn't move the stack pointer, that's done by using nextMethodEntry!
     *
     * @param entry    the entry point in the current method for resume
     * @param numSlots the number of required stack slots for storing the state of the current method
     */
    public final void pushMethod(int entry, int numSlots) {
        frames[sp] = new Frame(entry, numSlots);
    }

    /**
     * Called when a method is left (but not by suspending) and
     * the stack needs to be cleaned up.
     */
    public final void popMethod() {
        assert (sp != EMPTY) : "Can't pop empty stack.";
        frames[sp] = null;
        sp -= 1;
    }

    /**
     * called when resuming a stack
     *
     * this is enough since the containing coroutine / fiber is
     * also instrumented. Running it will jump into the right
     * entry point of the next method which in turn will jump
     * into the next entry point and so forth. This continues
     * until the shadow-stack is "copied" from the heap to
     * the actual JVM stack.
     */
    public final void resumeStack() {
        sp = EMPTY;
    }

    // Used by Quasar for testing/benchmarking only
    void resetStack() { resumeStack(); }

    /**
     * called when nextMethodEntry returns 0
     *
     * Ignore fast path for now and always return true!
     */
    public final boolean isFirstInStackOrPushed() {
        return true;
    }


    private static final ThreadLocal<Stack> globalStack = new ThreadLocal<Stack>() {
        @Override protected Stack initialValue() {
            return new Stack(null, INITIAL_METHOD_STACK_DEPTH);
        }
    };

    /**
     * Get the stack which is used by the curent fiber or continuation
     */
    public static Stack getStack() {
        final Continuation<?, ?> currentCont = Continuation.getCurrentContinuation();
        if (currentCont != null)
            return currentCont.getStack();
        final Fiber<?> currentFiber = Fiber.currentFiber();
        if (currentFiber != null)
            return currentFiber.stack;

        // this is a breaking change!
        // Existing implementations expect null here.
        return globalStack.get();
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Accessors">
    public final int getInt(int idx) { return currentFrame().getInt(idx); }

    public final float getFloat(int idx) { return currentFrame().getFloat(idx); }

    public final long getLong(int idx) { return currentFrame().getLong(idx); }

    public final double getDouble(int idx) { return currentFrame().getDouble(idx); }

    public final Object getObject(int idx) { return currentFrame().getObject(idx); }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Specialized Push Methods">
    public static void push(int value, Stack s, int idx) { s.currentFrame().push(value, idx); }
    public static void push(float value, Stack s, int idx) { s.currentFrame().push(value, idx); }
    public static void push(long value, Stack s, int idx) { s.currentFrame().push(value, idx); }
    public static void push(double value, Stack s, int idx) { s.currentFrame().push(value, idx); }
    public static void push(Object ref, Stack s, int idx) { s.currentFrame().push(ref, idx); }
    //</editor-fold>

    @Override
    public String toString() {
        StringBuffer out = new StringBuffer();
        out.append("=============\n");
        out.append("Stack (sp=" + sp + ")\n");

        int i = 0;

        while (i < frames.length && frames[i] != null) {
            out.append("-------------\n");
            if (i == sp) {
                out.append("*** ");
            }
            out.append(frames[i].toString());
            i += 1;
        }
        out.append("=============\n");

        return out.toString();
    }

    public void dump() {
        System.out.println(this.toString());
    }

    private void growStack(int requiredIndex) {
        final int oldSize = frames.length;
        int newSize = frames.length;

        while (newSize <= requiredIndex) {
            newSize *= 2;
        }

        if (newSize > oldSize) {
            frames = Arrays.copyOf(frames, newSize);
        }
    }

    private void clearAbove(int index) {
        Arrays.fill(frames, index + 1, frames.length, null);
    }

    public final class Marker implements Serializable {
        public final int pointer;

        public Marker(int pointer) {
            this.pointer = pointer;
        }

        @Override
        public String toString() {
            return "Marker(" + pointer + ")";
        }
    }

    //<editor-fold defaultstate="collapsed" desc="Fields and methods only needed for compatibility">
    public static final int MAX_ENTRY = (1 << 14) - 1;
    public static final int MAX_SLOTS = (1 << 16) - 1;

    private final Object context;

    private Object suspendedContext;
    void setSuspendedContext(Object context) {
        this.suspendedContext = context;
    }

    Fiber getFiber() {
        return context instanceof Fiber ? (Fiber) context : null;
    }

    public Continuation getAndClearSuspendedContinuation() {
        Object c = getSuspendedContext();
        setSuspendedContext(null);
        return (Continuation) c;
    }

    Object getSuspendedContext() {
        return suspendedContext;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Bit Utils for compatibility">
    ///////////////////////////////////////////////////////////////
    private static final long MASK_FULL = 0xffffffffffffffffL;

    private static long getUnsignedBits(long word, int offset, int length) {
        int a = 64 - length;
        int b = a - offset;
        return (word >>> b) & (MASK_FULL >>> a);
    }

    private static long getSignedBits(long word, int offset, int length) {
        int a = 64 - length;
        int b = a - offset;
        long xx = (word >>> b) & (MASK_FULL >>> a);
        return (xx << a) >> a; // set sign
    }

    private static long setBits(long word, int offset, int length, long value) {
        int a = 64 - length;
        int b = a - offset;
        //long mask = (MASK_FULL >>> a);
        word = word & ~((MASK_FULL >>> a) << b); // clears bits in our region [offset, offset+length)
        // value = value & mask;
        word = word | (value << b);
        return word;
    }

    private static boolean getBit(long word, int offset) {
        return (getUnsignedBits(word, offset, 1) != 0);
    }

    private static long setBit(long word, int offset, boolean value) {
        return setBits(word, offset, 1, value ? 1 : 0);
    }
    //</editor-fold>

}

class Frame implements Serializable {

    String name = ""; // Frames can be named for better debug output
    final int size;
    final int entry;
    final long[] values;
    final Object[] refs;

    Frame(int entry, int size) {
        this.entry = entry;
        this.size = size;
        this.values = new long[size];
        this.refs   = new Object[size];
    }

    @Override public String toString() {
        StringBuffer out = new StringBuffer();
        out.append("Frame " + name + "(entry=" + entry + ", size=" + size + ")\n");

        for (int i = 0; i < size; i++) {
            out.append("\tsp=" + i + ": val=" + values[i] + " ref=" + refs[i] + "\n");
        }

        return out.toString();
    }

    public void setName(String n) {
        this.name = n;
    }

    //<editor-fold defaultstate="collapsed" desc="Accessors">
    final int getInt(int idx) {
        return (int) values[idx];
    }
    final float getFloat(int idx) {
        return Float.intBitsToFloat((int) values[idx]);
    }
    final long getLong(int idx) {
        return values[idx];
    }
    final double getDouble(int idx) {
        return Double.longBitsToDouble(values[idx]);
    }
    final Object getObject(int idx) {
        return refs[idx];
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Pushs">
    void push(int value, int idx) { values[idx] = value; }
    void push(float value, int idx) { values[idx] = Float.floatToRawIntBits(value); }
    void push(long value, int idx) { values[idx] = value; }
    void push(double value, int idx) { values[idx] = Double.doubleToRawLongBits(value); }
    void push(Object ref, int idx) { refs[idx] = ref; }
    //</editor-fold>
}