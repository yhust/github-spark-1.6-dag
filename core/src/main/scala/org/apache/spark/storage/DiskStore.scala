/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.io.{IOException, File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import net.smacke.jaydio.DirectRandomAccessFile // for direct i/o in linux system
/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with Logging {

  val minMemoryMapBytes = blockManager.conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  override def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    logDebug(s"Attempting to put block $blockId")
    if(blockId.isRDD) {
      logInfo(s"yyh-DiskStore: putting Bytes of $blockId directly to the disk")
      bytes.rewind()
      val read_size = 1 << 23 // 8 MiB each time at most
      val file = diskManager.getFile(blockId)
      val direct_write = new DirectRandomAccessFile(file, "rw")// , read_size)
      val byte_array = new Array[Byte](read_size)
      val startTime = System.currentTimeMillis
      while (bytes.hasRemaining()) {
        val this_size = scala.math.min(read_size, bytes.remaining())
        bytes.get(byte_array, 0, this_size)
        direct_write.write(byte_array, 0, this_size)
      }
      val finishTime = System.currentTimeMillis
      logDebug(s"yyh-DiskStore: Block $blockId stored directly in the disk in %d ms"
                        .format(finishTime - startTime))
      direct_write.close()
      PutResult(bytes.limit(), Right(bytes.duplicate()))

    } else {
      val startTime = System.currentTimeMillis
      val file = diskManager.getFile(blockId)
      val channel = new FileOutputStream(file).getChannel
      Utils.tryWithSafeFinally {
        while (bytes.remaining > 0) {
          channel.write(bytes)
        }
      } {
        channel.close()
      }
      val finishTime = System.currentTimeMillis
      logDebug("Block %s stored as %s file on disk in %d ms".format(
        file.getName, Utils.bytesToString(bytes.limit), finishTime - startTime))
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    }
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {

    logDebug(s"Attempting to write values for block $blockId")
    logInfo(s"yyh: Writing $blockId as iterator to disk")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val outputStream = new FileOutputStream(file)
    try {
      Utils.tryWithSafeFinally {
        blockManager.dataSerializeStream(blockId, outputStream, values)
      } {
        // Close outputStream here because it should be closed before file is deleted.
        outputStream.close()
      }
    } catch {
      case e: Throwable =>
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
        throw e
    }

    val length = file.length

    val timeTaken = System.currentTimeMillis - startTime
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(length), timeTaken))

    if (returnValues) {
      // Return a byte buffer for the contents of the file
      val buffer = getBytes(blockId).get
      PutResult(length, Right(buffer))
    } else {
      PutResult(length, null)
    }
  }

  private def getBytes(file: File, offset: Long, length: Long, blockId: BlockId):
                                                Option[ByteBuffer] = {
    if (blockId.isRDD) {
      logInfo(s"yyh-DiskStore: Getting Bytes of $blockId directly from disk")
      val byte_buffer = ByteBuffer.allocate(length.toInt)
      val read_size = 1 << 23 // read 8 MiB a time (at most)
      val direct_read = new DirectRandomAccessFile(file, "r") // , read_size)
      val startTime = System.currentTimeMillis
      while(direct_read.getFilePointer() < direct_read.length()){
        val remaining = scala.math.min(read_size, direct_read.length()-direct_read.getFilePointer())
        val this_read_buf = new Array[Byte](remaining.toInt)
        direct_read.read(this_read_buf, 0, remaining.toInt)
        byte_buffer.put(this_read_buf, 0, this_read_buf.length)
        Thread.sleep(100) // wait for the read to complete
      }
      byte_buffer.rewind()
      direct_read.close()
      val finishTime = System.currentTimeMillis
      logInfo("Got bytes of Block %s on disk in %d ms".format(file.getName, finishTime - startTime))
      Some(byte_buffer)
    } else {
      logInfo(s"yyh-DiskStore: Getting Bytes of $blockId from disk")
      val channel = new RandomAccessFile(file, "r").getChannel
      val startTime = System.currentTimeMillis
      Utils.tryWithSafeFinally {
        // For small files, directly read rather than memory map
        if (length < minMemoryMapBytes) {
          val buf = ByteBuffer.allocate(length.toInt)
          channel.position(offset)
          while (buf.remaining() != 0) {
            if (channel.read(buf) == -1) {
              throw new IOException("Reached EOF before filling buffer\n" +
                s"offset=$offset\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
            }
          }
          buf.flip()
          Some(buf)
        } else {
          Some(channel.map(MapMode.READ_ONLY, offset, length))
        }
      } {
        val finishTime = System.currentTimeMillis
        logInfo("Got bytes of Shuffle Block %s on disk in %d ms".
            format(file.getName, finishTime - startTime))
        channel.close()
      }
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = diskManager.getFile(blockId.name)
    getBytes(file, 0, file.length, blockId)
  }

  // def getBytes(segment: FileSegment): Option[ByteBuffer] = {
    // getBytes(segment.file, segment.offset, segment.length)
  // }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    logInfo(s"yyh-DiskStore: Getting values of $blockId from disk")
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  override def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
