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
 *
 * Example Usage
 * -------------
 * Create an empty stack
 *
 *     s = new Stack(null, 16)
 *
 * Enter our first method, will return 0 since we never entered this method
 * before:
 *
 *     s.nextMethodEntry()                   m=0 entry=0 sp=1 slots=0 prevSlots=0
 *     ... do some work ...
 *
 * Save the state of this method, continues with label 1, and makes space for
 * 2 registers
 *     s.pushMethod(1, 2)                    m=0 entry=1 sp=1 slots=2 prevSlots=0
 *     Stack.push(true, s, 0)
 *     Stack.push(2, s, 1)                   m=0 entry=1 sp=1 slots=2 prevSlots=0
 *                                                   sp=1 long=0 obj=true
 *                                                   sp=2 long=2 obj=null
 *
 * Enter another method, again will return 0
 *
 *     s.nextMethodEntry()                  m=0 entry=1 sp=1 slots=2 prevSlots=0
 *                                                   sp=1 long=0 obj=true
 *                                                   sp=2 long=2 obj=null
 *                                          m=1 entry=0 sp=4 slots=0 prevSlots=2
 *     ... do some work ...
 *
 * Also save the state for this method, continues with label 7, and makes
 * space for 1 register
 *
 *     s.pushMethod(7, 1)
 *     Stack.push(42, s, 0)                 m=0 entry=1 sp=1 slots=2 prevSlots=0
 *                                                  sp=1 long=0 obj=true
 *                                                  sp=2 long=2 obj=null
 *                                          m=1 entry=7 sp=4 slots=1 prevSlots=2
 *                                                  sp=4 long=42 obj=null
 *
 * Reset the stack pointer to point to the base (sp=0)
 *
 *     s.resumeStack
 *
 *
 * A method thus has the shape:
 *
 * void foo() {
 *
 *   pc = s.nextMethodEntry()
 *   switch (pc) {
 *      case 1:
 *         ...
 *      case 2:
 *         ...
 *      default:
 *         firstEntry = s.isFirstInStackOrPushed
 *
 *         if (!firstEntry) {
 *           // the stack is set to null to remember
 *           // there is nothing we need to store.
 *           s = null
 *         }
 *   }
 *
 *
 * }
 *
 *
 * @author Matthias Mann
 * @author Ron Pressler
 */
public final class Stack implements Serializable {

    //<editor-fold defaultstate="collapsed" desc="Constants">
    public static final int MAX_ENTRY = (1 << 14) - 1;
    public static final int MAX_SLOTS = (1 << 16) - 1;
    private static final int INITIAL_METHOD_STACK_DEPTH = 16;
    private static final int FRAME_RECORD_SIZE = 1;
    private static final int EMPTY = -1;
    private static final long serialVersionUID = 12786283751253L;
    private static final ThreadLocal<Stack> globalStack = new ThreadLocal<Stack>() {
        @Override protected Stack initialValue() {
            return new Stack(null, INITIAL_METHOD_STACK_DEPTH);
        }
    };
    private final Object context;
    //</editor-fold>

    /*
     * sp points to the first slot to contain data.
     * The _previous_ FRAME_RECORD_SIZE slots contain the frame record.
     * The frame record currently occupies a single long:
     *   - entry (PC)         : 14 bits
     *   - num slots          : 16 bits
     *   - prev method slots  : 16 bits
     */
    // we use -1 as a special value to indicate the empty or freshly resumed stack
    // the first frame record is stored at 0 and the first data is stored at 1
    int sp = EMPTY;
    long[] dataLong;        // holds primitives on stack as well as each method's entry point and the stack pointer
    Object[] dataObject;    // holds refs on stack

    //<editor-fold defaultstate="collapsed" desc="Constructors">

    public Stack(int stackSize) {
        this(null, stackSize);
    }

    public Stack(Object context, int stackSize) {
        if (stackSize <= 0)
            throw new IllegalArgumentException("stackSize: " + stackSize);

        this.context = context;
        this.dataLong = new long[stackSize + (FRAME_RECORD_SIZE * INITIAL_METHOD_STACK_DEPTH)];
        this.dataObject = new Object[stackSize + (FRAME_RECORD_SIZE * INITIAL_METHOD_STACK_DEPTH)];

        resumeStack();
    }

    public Stack(Object context, Stack s) {
        this.context = context;
        this.dataLong = Arrays.copyOf(s.dataLong, s.dataLong.length);
        this.dataObject = Arrays.copyOf(s.dataObject, s.dataObject.length);
        resumeStack();
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="DelimCC Operations">
    public final Marker currentFrame() {
        if (sp == EMPTY)
            return new Marker(EMPTY);
        else
            return new Marker(sp - FRAME_RECORD_SIZE);
    }

    public final void resumeAt(Marker frame) {
        System.out.println("resume at " + frame);
        sp = frame.pointer + FRAME_RECORD_SIZE;
    }

    /**
     * Splits the stack at position frame, copies everything into
     * a stack segment and zeros out the next frame record.
     * 
     * @param frame
     * @return the segment above frame
     */
    public final Segment popSegmentAbove(Marker frame) {
        System.out.println("popSegmentAbove " + frame);

        if (frame.pointer > sp)
            throw new IllegalArgumentException("Can't copy something above stack pointer: " + sp + ". Marker is " + frame.pointer);

        // we are already done if there is nothing to copy.
        if (sp == EMPTY)
            return new Segment();

        final int fromIdx = Math.max(frame.pointer, 0);
        final int toIdx =  sp + getNumSlots(currentFrameRecord());
        final int oldSp = sp;

        long[] values = Arrays.copyOfRange(dataLong, fromIdx, toIdx);
        Object[] refs = Arrays.copyOfRange(dataObject, fromIdx, toIdx);

        // first frame in the copied segment
        final long firstFrame = dataLong[fromIdx];

        // last frame in the stack after popping segment
        sp = fromIdx - getPrevNumSlots(firstFrame);

        // clear the next available frame record for next entry
        dataLong[fromIdx] = 0L;

        // help garbage collector and clear references
        Arrays.fill(dataObject, fromIdx, toIdx, null);

        return new Segment(values, refs, oldSp - sp);
    }

    public final void pushSegment(Segment s) {
        System.out.println("pushSegment " + s);

        // (1) make enough space available to push Segment
        long curr = currentFrameRecord();
        int currSlots = getNumSlots(curr);
        int firstIdx = sp + currSlots;
        int lastFrame = firstIdx + s.values.length;
        growStack(lastFrame + FRAME_RECORD_SIZE);

        // (2) copy segment
        System.arraycopy(s.values, 0, dataLong, firstIdx, s.values.length);
        System.arraycopy(s.refs, 0, dataObject, firstIdx, s.refs.length);

        // (3) update prevSlots in next slots of segment
        dataLong[firstIdx] = setPrevNumSlots(dataLong[firstIdx], currSlots);

        // (4) clear next slot after copied segment
        dataLong[lastFrame] = 0L;

        // (5) set stack pointer to last frame on stack
        sp += s.sp;
    }

    //</editor-fold>

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

    // for testing/benchmarking only
    void resetStack() {
        resumeStack();
    }

    /**
     * Called at the beginning of a method to return the
     * jump-label of the next entry point or 0, if
     * the method is being entered for the first time.
     *
     * Either the entry has been pushed before by usage of
     * "pushMethod" or it is assumed to be 0
     *
     * Also changes the sp to point to the next frame
     *
     * @return the entry point of this method
     */
    public final int nextMethodEntry() {
        System.out.println("nextMethodEntry");
        // this is the very first entry (potentially since a resume)
        if (sp == EMPTY) {
            sp = FRAME_RECORD_SIZE;
            return getEntry(currentFrameRecord());
        }

        final long prev = currentFrameRecord();

        // this is a special case!
        // the previous slot is unused (probably because it is the
        // very first slot on the stack and we never entered a method
        // so far), so we just use it and return 0;
        if (isEmpty(prev))
            return 0;

        moveToNextFrame();
        updateFrameRecord(getNumSlots(prev));
        return getEntry(currentFrameRecord());
    }

    // increment the sp pointer to point to the next frame
    private final void moveToNextFrame() {
        final int prevSlots = getNumSlots(currentFrameRecord());
        final int nextIndex = sp + prevSlots;
        sp = nextIndex + FRAME_RECORD_SIZE;
    }

    private final long currentFrameRecord() {
        // TODO check what's the sensible solution here
        if (sp <= 0) return 0L;

        return dataLong[sp - FRAME_RECORD_SIZE];
    }

    /**
     * Updates the current frame record by setting the number of slots
     * of the previous slot. It is necessary to do this early, in case
     * an exception is being thrown before the corresponding pushMethod
     * happens.
     */
    private final void updateFrameRecord(int slots) {
        final int idx = sp - FRAME_RECORD_SIZE;
        dataLong[idx] = setPrevNumSlots(dataLong[idx], slots);
    }

    /**
     * called when nextMethodEntry returns 0
     *
     * Ignore fast path for now!
     */
    public final boolean isFirstInStackOrPushed() {
        return true;
    }

    /**
     * Called before another, effectful method is called.
     * Reserves enough space on the stack to stores the state of the
     * current method, before entering the other method.
     *
     * @param entry    the entry point in the current method for resume
     * @param numSlots the number of required stack slots for storing the state of the current method
     */
    public final void pushMethod(int entry, int numSlots) {
        System.out.println("pushMethod. entry: " + entry + " numSlots " + numSlots);
        if (sp == EMPTY)
            throw new RuntimeException("can't push a method, before a method is entered. Use nextMethodEntry, before pushMethod.");

        int idx = sp - FRAME_RECORD_SIZE;
        long record = dataLong[idx];
        record = setEntry(record, entry);
        record = setNumSlots(record, numSlots);
        dataLong[idx] = record;

        int nextMethodIdx = sp + numSlots;
        int nextMethodSP = nextMethodIdx + FRAME_RECORD_SIZE;
        if (nextMethodSP > dataObject.length)
            growStack(nextMethodSP);

        // clear next method's frame record to guarantee that
        // the entry point is 0.
        dataLong[nextMethodIdx] = 0L;
    }

    /**
     * Called when a method is left (but not by suspending) and
     * the stack needs to be cleaned up.
     */
    public final void popMethod() {
        System.out.println("popMethod");
        if (sp <= 0) {
            dump();
            throw new RuntimeException("can't pop method with sp at " + sp);
        }

        final int oldSP = sp;
        final int idx = oldSP - FRAME_RECORD_SIZE;
        final long record = dataLong[idx];
        final int slots = getNumSlots(record);
        final int newSP = idx - getPrevNumSlots(record);

        // clear frame record
        // ---
        // this is essential, since the entry point needs to be 0
        // next time we encounter a new method call.
        //
        // however it could be redundant, since pushMethod also
        // always clears the next frame record.
        dataLong[idx] = 0L;

        // help GC
        Arrays.fill(dataObject, oldSP, oldSP + slots, null);

        sp = newSP;
    }

    private void growStack(int required) {
        int newSize = dataObject.length;
        do {
            newSize *= 2;
        } while (newSize < required);

        dataLong = Arrays.copyOf(dataLong, newSize);
        dataObject = Arrays.copyOf(dataObject, newSize);
    }

    public static Stack getStack() {
        final Continuation<?, ?> currentCont = Continuation.getCurrentContinuation();
        if (currentCont != null)
            return currentCont.getStack();
        final Fiber<?> currentFiber = Fiber.currentFiber();
        if (currentFiber != null)
            return currentFiber.stack;
        return globalStack.get();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(this)) + "{sp: " + sp + "}";
    }

    public void dump() {
        int m = 0;
        int k = 0;
        while (k < sp) {
            final long record = dataLong[k++];
            final int slots = getNumSlots(record);

            System.out.println("\tm=" + (m++) + " entry=" + getEntry(record) + " sp=" + k + " slots=" + slots + " prevSlots=" + getPrevNumSlots(record));
            for (int i = 0; i < slots; i++, k++)
                System.out.println("\t\tsp=" + k + " long=" + dataLong[k] + " obj=" + dataObject[k]);
        }
    }

    public Stack clone() {
        Stack copy = new Stack(context, this);
        copy.sp = this.sp;
        return copy;
    }

    //<editor-fold defaultstate="collapsed" desc="Specialized Push Methods">
    /////////// Specialized Push Methods ///////////////////////////////////
    public static void push(int value, Stack s, int idx) {
        s.dataLong[s.sp + idx] = value;
    }

    public static void push(float value, Stack s, int idx) {
        s.dataLong[s.sp + idx] = Float.floatToRawIntBits(value);
    }

    public static void push(long value, Stack s, int idx) {
        s.dataLong[s.sp + idx] = value;
    }

    public static void push(double value, Stack s, int idx) {
        s.dataLong[s.sp + idx] = Double.doubleToRawLongBits(value);
    }

    public static void push(Object value, Stack s, int idx) {
        s.dataObject[s.sp + idx] = value;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Specialized Stack Accessors">
    /////////// Specialized Stack Accessors ///////////////////////////////////
    public final int getInt(int idx) {
        return (int) dataLong[sp + idx];
    }

    public final float getFloat(int idx) {
        return Float.intBitsToFloat((int) dataLong[sp + idx]);
    }

    public final long getLong(int idx) {
        return dataLong[sp + idx];
    }

    public final double getDouble(int idx) {
        return Double.longBitsToDouble(dataLong[sp + idx]);
    }

    public final Object getObject(int idx) {
        return dataObject[sp + idx];
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Frame Entry Methods">
    ///////////////////////////////////////////////////////////////
    private static long setEntry(long record, int entry) {
        return setBits(record, 0, 14, entry);
    }

    private static int getEntry(long record) {
        return (int) getUnsignedBits(record, 0, 14);
    }

    private static long setNumSlots(long record, int numSlots) {
        return setBits(record, 14, 16, numSlots);
    }

    private static int getNumSlots(long record) {
        return (int) getUnsignedBits(record, 14, 16);
    }

    private static long setPrevNumSlots(long record, int numSlots) {
        return setBits(record, 30, 16, numSlots);
    }

    private static int getPrevNumSlots(long record) {
        return (int) getUnsignedBits(record, 30, 16);
    }

    private static boolean isEmpty(long record) {
        return getEntry(record) == 0;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Bit Utils">
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

    //<editor-fold defaultstate="collapsed" desc="Suspended Context">
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

    /**
     * Important: The first frame on the stack-segment still has a "prevSlots"
     *            count that might be out-of date, when repushed. We need to
     *            keep that updated.
     */
    public final class Segment implements Serializable {
        final long[] values;  // holds primitives on stack as well as each method's entry point and the stack pointer
        final Object[] refs;  // holds refs on stack
        final int sp;         // relative stack pointer

        Segment() {
            this.values = new long[] {};
            this.refs = new Object[] {};
            this.sp = EMPTY;
        }

        Segment(long[] values, Object[] refs, int sp) {
            this.values = values;
            this.refs = refs;
            this.sp = sp;
        }

        @Override
        public String toString() {
            StringBuffer out = new StringBuffer();
            out.append("Segment(sp = " + sp + ")\n");
            int m = 0;
            int k = 0;
            while (k < sp) {
                final long record = values[k++];
                final int slots = getNumSlots(record);

                out.append("\tm=" + (m++) + " entry=" + getEntry(record) + " sp=" + k + " slots=" + slots + " prevSlots=" + getPrevNumSlots(record) + "\n");
                for (int i = 0; i < slots; i++, k++)
                    out.append("\t\tsp=" + k + " long=" + values[k] + " obj=" + refs[k] + "\n");
            }
            return out.toString();
        }
    }

    public final class Marker implements Serializable {
        final int pointer;

        Marker(int pointer) {
            this.pointer = pointer;
        }

        @Override
        public String toString() {
            return "Marker(" + pointer + ")";
        }
    }
}
