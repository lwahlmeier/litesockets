package org.threadly.litesockets.utils;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;

import org.threadly.util.ArgumentVerifier;

/**
 * This class is used to combine multiple ByteBuffers into 1 simplish to use interface.
 * It provides most of the features of a single ByteBuffer, but with the ability to perform those 
 * operations spanning many ByteBuffers.
 * 
 * The idea here is to keep from having to copy around and merge ByteBuffers as much as possible. 
 * 
 * NOTE: This is not threadSafe.  It should only be accessed by 1 thread at a time.
 * 
 */
public class MergedByteBuffers {
  public static final int BYTES_IN_LONG = Long.SIZE/Byte.SIZE;
  public static final int BYTES_IN_INT = Integer.SIZE/Byte.SIZE;
  public static final int BYTES_IN_SHORT = Short.SIZE/Byte.SIZE;
  
  public static final short UNSIGNED_BYTE_MASK = 0xff;
  public static final int UNSIGNED_SHORT_MASK = 0xffff;
  public static final long UNSIGNED_INT_MASK = 0xffffffffL;
  
  protected final ArrayDeque<ByteBuffer> availableBuffers = new ArrayDeque<ByteBuffer>();
  protected volatile int currentSize = 0;
  protected volatile long consumedSize = 0;

  /**
   * This method allows you to add ByteBuffers to the MergedByteBuffers.  
   * All must be done in order of how you want to pull the data back out.
   * 
   * @param buffer - The byte buffer to add to the MergedByteBuffers
   */
  public void add(ByteBuffer buffer) {
    if(buffer.hasRemaining()) {
      availableBuffers.add(buffer.duplicate().asReadOnlyBuffer());
      currentSize+=buffer.remaining();
    } 
  }
  
  public MergedByteBuffers copy() {
    MergedByteBuffers mbb  = new MergedByteBuffers();
    for(ByteBuffer bb: this.availableBuffers) {
      mbb.add(bb.duplicate().asReadOnlyBuffer());
    }
    return mbb;
  }
  
  /**
   * This method allows you to add a MergedByteBuffers to another MergedByteBuffers.  
   * All must be done in order of how you want to pull the data back out.
   * 
   * @param mbb - The MergedByteBuffers to put into this MergedByteBuffers
   */
  public void add(MergedByteBuffers mbb) {
    for(ByteBuffer bb: mbb.availableBuffers) {
      add(bb);
    }
    mbb.availableBuffers.clear();
    mbb.consumedSize += mbb.currentSize;
    mbb.currentSize = 0;
  }
  
  /**
   * This will flush all the data in this MergedByteBuffer into another MergedByteBuffer.
   * 
   * @return a new MergedByteBuffer with the data that was in the original one.
   */
  public MergedByteBuffers duplicateAndClean() {
    MergedByteBuffers mbb = new MergedByteBuffers();
    mbb.add(this);
    return mbb;
  }

  /**
   * Like the indexOf in String object this find a pattern of bytes and reports the position they start at.
   * 
   * @param pattern String pattern to search for
   * @return an {@code int} with the offset of the first occurrence of the given . 
   */
  public int indexOf(String pattern) {
    ArgumentVerifier.assertNotNull(pattern, "String");
    return indexOf(pattern.getBytes());
  }
  
  /**
   * Like the indexOf in String object this find a pattern of bytes and reports the position they start at.
   * 
   * @param pattern byte[] pattern to search for
   * @return an {@code int} with the offset of the first occurrence of the given . 
   */
  public int indexOf(byte[] pattern) {
    ArgumentVerifier.assertNotNull(pattern, "byte[]");
    if(currentSize == 0){
      return -1;
    }

    int patPos = 0;
    int bufPos = 0;

    MergedByteBuffers mbb = copy();

    while(mbb.remaining() >= pattern.length-patPos) {
      if(pattern[patPos] == mbb.get()) {
        if(patPos == pattern.length-1) {
          return bufPos;
        }
        patPos++;
      } else {
        bufPos++;
        if (patPos != 0) {
          mbb = copy();
          mbb.discard(bufPos);
        }
        patPos = 0;
      }
    }
    return -1;
  }


  /**
   * Check how much data is available in the MergedByteBuffer.
   * 
   * @return the current about of space remaining in the MergedByteBuffer.
   */
  public int remaining() {
    return currentSize;
  }

  /**
   * Returns the next byte stored in the MergedByteBuffer.
   * 
   * @return the next single Byte from the MergedByteBuffer.
   */
  public byte get() {
    if(currentSize == 0){
      throw new BufferUnderflowException();
    }
    ByteBuffer buf = availableBuffers.peek();

    // we assume that we have at least one byte in any available buffers
    byte result = buf.get();

    if (! buf.hasRemaining()) {
      removeFirstBuffer();
    }
    currentSize--;
    consumedSize++;
    return result;
  }
  
  /**
   * Returns the next {@code byte} unsigned as {@code short} stored in the MergedByteBuffer.
   * 
   * @return the next single unsigned {@code byte} as {@code short} from the MergedByteBuffer.
   */
  public short getUnsignedByte() {
    return (short)(get() & UNSIGNED_BYTE_MASK);
  }

  /**
   * Returns an unsigned {@code short} (as an {@code int}) from the next 2 stored bytes.
   * 
   * @return the next 2 byte as an {@code int} (unsigned Short)
   */
  public int getUnsignedShort() {
    return getShort() & UNSIGNED_SHORT_MASK;
  }

  /**
   * Returns the next 2 bytes as a {@code short} value.
   * 
   * @return {@code short} of the next 2 bytes.
   */
  public short getShort() {
    if (currentSize < BYTES_IN_SHORT) {
      throw new BufferUnderflowException();
    }
    return pull(BYTES_IN_SHORT).getShort();
  }

  /**
   * Returns the next 4 bytes as an {@code int} value.
   * 
   * @return an {@code int} from the next 4 bytes
   */
  public int getInt() {
    if (currentSize < BYTES_IN_INT) {
      throw new BufferUnderflowException();
    }
    return pull(BYTES_IN_INT).getInt();
  }

  /**
   * Returns an unsigned short (as an {@code int}) from the next 2 stored bytes.
   * 
   * @return the next 2 byte as an {@code int} (unsigned Short)
   */
  public long getUnsignedInt() {    
    return getInt() & UNSIGNED_INT_MASK;
  }

  /**
   * Returns the next 8 bytes as a {@code long} value.
   * 
   * @return a {@code long} from the next 8 bytes.
   */
  public long getLong() {
    if (currentSize < BYTES_IN_LONG) {
      throw new BufferUnderflowException();
    }
    return pull(BYTES_IN_LONG).getLong();
  }
  

  /**
   * Fills the passed {@code byte[]} completely with data from the MergedByteBuffer. 
   * 
   * @param destBytes fills the given byteArray with the next bytes from the MergedByteBuffer.
   * 
   * @throws BufferUnderflowException if the {@code byte[]} is larger then the {@link #remaining()} in the MergedByteBuffer.
   */
  public void get(byte[] destBytes) {
    ArgumentVerifier.assertNotNull(destBytes, "byte[]");
    if (currentSize < destBytes.length) {
      throw new BufferUnderflowException();
    }
    doGet(destBytes);
    consumedSize += destBytes.length;
    currentSize -= destBytes.length;
  }
  
  
  /**
   * Get the size of the next full {@link ByteBuffer} in the queue.
   * 
   * @return the size of the next {@link ByteBuffer} in the queue.
   */
  public int nextPopSize() {
    if (currentSize == 0) {
      return 0;
    }
    return availableBuffers.peekFirst().remaining();
  }
  
  /**
   * Get the next Complete {@link ByteBuffer} in its entirety.  This byteBuffer could be 
   * any size and it will just pull it off the queue and return it.
   * 
   * If {@link #remaining()} is 0 you will get an empty {@link ByteBuffer}.
   * 
   * @return the next byteBuffer in the queue.
   */
  public ByteBuffer pop() {
    if (currentSize == 0) {
      return ByteBuffer.allocate(0);
    }
    return pull(availableBuffers.peekFirst().remaining());
  }

  /**
   * 
   * @param size size of the {@link ByteBuffer} to pull out of the MergedByteBuffer.
   * 
   * @return a {@link ByteBuffer} of %SIZE% bytes.
   */
  public ByteBuffer pull(int size) {
    ArgumentVerifier.assertNotNegative(size, "size");
    if (size == 0) {
      return ByteBuffer.allocate(0);
    }
    if (currentSize < size) {
      throw new BufferUnderflowException();
    }
    consumedSize += size;
    currentSize -= size;
    ByteBuffer first = availableBuffers.peek();
    if(first.remaining() == size) {
      ByteBuffer result = removeFirstBuffer().slice();
      return result;
    } else if(first.remaining() > size) {
      ByteBuffer bb = first.duplicate().slice();
      bb.limit(bb.position()+size);
      first.position(first.position()+size);
      return bb;
    } else {
      byte[] result = new byte[size];
      doGet(result);
      return ByteBuffer.wrap(result);
    }
  }

  /**
   * Discard will drop how ever many bytes you tell it to on the floor.
   * 
   * @param size the number of bytes to discard.
   */
  public void discard(int size) {
    ArgumentVerifier.assertNotNegative(size, "size");
    if (currentSize < size) {
      throw new BufferUnderflowException();
    }
    //We have logic here since we dont need to do any copying and we just drop the bytes
    int toRemoveAmount = size;
    while (toRemoveAmount > 0) {
      ByteBuffer buf = availableBuffers.peek();
      int bufRemaining = buf.remaining();
      if (bufRemaining > toRemoveAmount) {
        buf.position(buf.position() + toRemoveAmount);
        toRemoveAmount = 0;
      } else {
        removeFirstBuffer();
        toRemoveAmount -= bufRemaining;
      }
    }
    consumedSize += size;
    currentSize -= size;
  }

  /**
   * This will return the specified number of bytes as a String object.
   * 
   * @param size the number of bytes to put into the string.
   * @return as String Object with set number of bytes in it.
   */
  public String getAsString(int size) {
    ArgumentVerifier.assertNotNegative(size, "size");
    byte[] ba = new byte[size];
    get(ba);
    return new String(ba);
  }
  
  protected ByteBuffer removeFirstBuffer() {
    return this.availableBuffers.pollFirst();
  }
  
  private void doGet(byte[] destBytes) {
    int remainingToCopy = destBytes.length;

    while (remainingToCopy > 0) {
      ByteBuffer buf = availableBuffers.peek();

      int toCopy = Math.min(buf.remaining(), remainingToCopy);
      buf.get(destBytes, destBytes.length - remainingToCopy, toCopy);
      remainingToCopy -= toCopy;

      if (! buf.hasRemaining()) {
        removeFirstBuffer();
      }
    }
  }

  
  public long getTotalConsumedBytes() {
    return consumedSize;
  }
}
