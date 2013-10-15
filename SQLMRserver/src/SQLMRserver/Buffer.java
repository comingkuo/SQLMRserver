/*
 Copyright (c) 2002, 2010, Oracle and/or its affiliates. All rights reserved.
 

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA



 */
package SQLMRserver;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 * Buffer contains code to read and write packets from/to the MySQL server.
 * 
 * @version $Id$
 * @author Mark Matthews
 */
public class Buffer {
	private int bufLength = 0;

	private byte[] byteBuffer;

	private int position = 0;

	Buffer(byte[] buf) {
		this.byteBuffer = buf;
		setBufLength(buf.length);
	}

	//modify in SQLMR
	Buffer(int size) {
		this.byteBuffer = new byte[size];
		setBufLength(this.byteBuffer.length);
		this.position = 0;//MysqlIO.HEADER_LENGTH;
	}

	//modify in SQLMR
	final void clear() {
		this.position = 0;//MysqlIO.HEADER_LENGTH;
	}



	final void dumpNBytes(int start, int nBytes) {
		StringBuffer asciiBuf = new StringBuffer();

		for (int i = start; (i < (start + nBytes)) && (i < getBufLength()); i++) {
			String hexVal = Integer.toHexString(readByte(i) & 0xff);

			if (hexVal.length() == 1) {
				hexVal = "0" + hexVal; //$NON-NLS-1$
			}

			System.out.print(hexVal + " "); //$NON-NLS-1$

			if ((readByte(i) > 32) && (readByte(i) < 127)) {
				asciiBuf.append((char) readByte(i));
			} else {
				asciiBuf.append("."); //$NON-NLS-1$
			}

			asciiBuf.append(" "); //$NON-NLS-1$
		}

		System.out.println("    " + asciiBuf.toString()); //$NON-NLS-1$
	}

	final void ensureCapacity(int additionalData) throws SQLException {
		if ((this.position + additionalData) > getBufLength()) {
			if ((this.position + additionalData) < this.byteBuffer.length) {
				// byteBuffer.length is != getBufLength() all of the time
				// due to re-using of packets (we don't shrink them)
				//
				// If we can, don't re-alloc, just set buffer length
				// to size of current buffer
				setBufLength(this.byteBuffer.length);
			} else {
				//
				// Otherwise, re-size, and pad so we can avoid
				// allocing again in the near future
				//
				int newLength = (int) (this.byteBuffer.length * 1.25);

				if (newLength < (this.byteBuffer.length + additionalData)) {
					newLength = this.byteBuffer.length
							+ (int) (additionalData * 1.25);
				}

				if (newLength < this.byteBuffer.length) {
					newLength = this.byteBuffer.length + additionalData;
				}

				byte[] newBytes = new byte[newLength];

				System.arraycopy(this.byteBuffer, 0, newBytes, 0,
						this.byteBuffer.length);
				this.byteBuffer = newBytes;
				setBufLength(this.byteBuffer.length);
			}
		}
	}

	
	protected final byte[] getBufferSource() {
		return this.byteBuffer;
	}

	int getBufLength() {
		return this.bufLength;
	}

	/**
	 * Returns the array of bytes this Buffer is using to read from.
	 * 
	 * @return byte array being read from
	 */
	public byte[] getByteBuffer() {
		return this.byteBuffer;
	}

	final byte[] getBytes(int len) {
		byte[] b = new byte[len];
		System.arraycopy(this.byteBuffer, this.position, b, 0, len);
		this.position += len; // update cursor

		return b;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mysql.jdbc.Buffer#getBytes(int, int)
	 */
	byte[] getBytes(int offset, int len) {
		byte[] dest = new byte[len];
		System.arraycopy(this.byteBuffer, offset, dest, 0, len);

		return dest;
	}

	int getCapacity() {
		return this.byteBuffer.length;
	}

	/**
	 * Returns the current position to write to/ read from
	 * 
	 * @return the current position to write to/ read from
	 */
	public int getPosition() {
		return this.position;
	}

	// 2000-06-05 Changed
	final boolean isLastDataPacket() {
		return ((getBufLength() < 9) && ((this.byteBuffer[0] & 0xff) == 254));
	}

	//modify in SQLMR 2013-7-17 Changed
	final boolean isLastPacket(int length) {
		return (getPosition() >= length);
	}
	final long newReadLength() {
		int sw = this.byteBuffer[this.position++] & 0xff;

		switch (sw) {
		case 251:
			return 0;

		case 252:
			return readInt();

		case 253:
			return readLongInt();

		case 254: // changed for 64 bit lengths
			return readLongLong();

		default:
			return sw;
		}
	}

	final byte readByte() {
		return this.byteBuffer[this.position++];
	}

	final byte readByte(int readAt) {
		return this.byteBuffer[readAt];
	}


	// 2000-06-05 Changed
	final int readInt() {
		byte[] b = this.byteBuffer; // a little bit optimization

		return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8);
	}

	final int readIntAsLong() {
		byte[] b = this.byteBuffer;

		return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8)
				| ((b[this.position++] & 0xff) << 16)
				| ((b[this.position++] & 0xff) << 24);
	}


	final long readLength() {
		int sw = this.byteBuffer[this.position++] & 0xff;

		switch (sw) {
		case 251:
			return 0;

		case 252:
			return readInt();

		case 253:
			return readLongInt();

		case 254:
			return readLong();

		default:
			return sw;
		}
	}

	// 2000-06-05 Fixed
	final long readLong() {
		byte[] b = this.byteBuffer;

		return ((long) b[this.position++] & 0xff)
				| (((long) b[this.position++] & 0xff) << 8)
				| ((long) (b[this.position++] & 0xff) << 16)
				| ((long) (b[this.position++] & 0xff) << 24);
	}

	// 2000-06-05 Changed
	final int readLongInt() {
		byte[] b = this.byteBuffer;

		return (b[this.position++] & 0xff) | ((b[this.position++] & 0xff) << 8)
				| ((b[this.position++] & 0xff) << 16);
	}

	// 2000-06-05 Fixed
	final long readLongLong() {
		byte[] b = this.byteBuffer;

		return (b[this.position++] & 0xff)
				| ((long) (b[this.position++] & 0xff) << 8)
				| ((long) (b[this.position++] & 0xff) << 16)
				| ((long) (b[this.position++] & 0xff) << 24)
				| ((long) (b[this.position++] & 0xff) << 32)
				| ((long) (b[this.position++] & 0xff) << 40)
				| ((long) (b[this.position++] & 0xff) << 48)
				| ((long) (b[this.position++] & 0xff) << 56);
	}

	final int readnBytes() {
		int sw = this.byteBuffer[this.position++] & 0xff;

		switch (sw) {
		case 1:
			return this.byteBuffer[this.position++] & 0xff;

		case 2:
			return this.readInt();

		case 3:
			return this.readLongInt();

		case 4:
			return (int) this.readLong();

		default:
			return 255;
		}
	}

	//
	// Read a null-terminated string
	//
	// To avoid alloc'ing a new byte array, we
	// do this by hand, rather than calling getNullTerminatedBytes()
	//
	final String readString() {
		int i = this.position;
		int len = 0;
		int maxLen = getBufLength();

		while ((i < maxLen) && (this.byteBuffer[i] != 0)) {
			len++;
			i++;
		}

		String s = new String(this.byteBuffer, this.position, len);
		this.position += (len + 1); // update cursor

		return s;
	}
	
	//add in SQLMR
	final String readString(int strLen) {	
		String s = new String(this.byteBuffer, this.position, strLen);
		this.position += strLen;
		return s;
	}

	


	void setBufLength(int bufLengthToSet) {
		this.bufLength = bufLengthToSet;
	}

	/**
	 * Sets the array of bytes to use as a buffer to read from.
	 * 
	 * @param byteBuffer
	 *            the array of bytes to use as a buffer
	 */
	public void setByteBuffer(byte[] byteBufferToSet) {
		this.byteBuffer = byteBufferToSet;
	}

	/**
	 * Set the current position to write to/ read from
	 * 
	 * @param position
	 *            the position (0-based index)
	 */
	public void setPosition(int positionToSet) {
		this.position = positionToSet;
	}

	public String toSuperString() {
		return super.toString();
	}

	final void writeByte(byte b) throws SQLException {
		ensureCapacity(1);

		this.byteBuffer[this.position++] = b;
	}

	// Write a byte array
	final void writeBytesNoNull(byte[] bytes) throws SQLException {
		int len = bytes.length;
		ensureCapacity(len);
		System.arraycopy(bytes, 0, this.byteBuffer, this.position, len);
		this.position += len;
	}

	// Write a byte array with the given offset and length
	final void writeBytesNoNull(byte[] bytes, int offset, int length)
			throws SQLException {
		ensureCapacity(length);
		System.arraycopy(bytes, offset, this.byteBuffer, this.position, length);
		this.position += length;
	}

	final void writeDouble(double d) throws SQLException {
		long l = Double.doubleToLongBits(d);
		writeLongLong(l);
	}

	final void writeFieldLength(long length) throws SQLException {
		if (length < 251) {
			writeByte((byte) length);
		} else if (length < 65536L) {
			ensureCapacity(3);
			writeByte((byte) 252);
			writeInt((int) length);
		} else if (length < 16777216L) {
			ensureCapacity(4);
			writeByte((byte) 253);
			writeLongInt((int) length);
		} else {
			ensureCapacity(9);
			writeByte((byte) 254);
			writeLongLong(length);
		}
	}

	final void writeFloat(float f) throws SQLException {
		ensureCapacity(4);

		int i = Float.floatToIntBits(f);
		byte[] b = this.byteBuffer;
		b[this.position++] = (byte) (i & 0xff);
		b[this.position++] = (byte) (i >>> 8);
		b[this.position++] = (byte) (i >>> 16);
		b[this.position++] = (byte) (i >>> 24);
	}

	// 2000-06-05 Changed
	final void writeInt(int i) throws SQLException {
		ensureCapacity(2);

		byte[] b = this.byteBuffer;
		b[this.position++] = (byte) (i & 0xff);
		b[this.position++] = (byte) (i >>> 8);
	}

	// Write a String using the specified character
	// encoding
	final void writeLenBytes(byte[] b) throws SQLException {
		int len = b.length;
		ensureCapacity(len + 9);
		writeFieldLength(len);
		System.arraycopy(b, 0, this.byteBuffer, this.position, len);
		this.position += len;
	}

	// Write a String using the specified character
	// encoding

	// 2000-06-05 Changed
	final void writeLong(long i) throws SQLException {
		ensureCapacity(4);

		byte[] b = this.byteBuffer;
		b[this.position++] = (byte) (i & 0xff);
		b[this.position++] = (byte) (i >>> 8);
		b[this.position++] = (byte) (i >>> 16);
		b[this.position++] = (byte) (i >>> 24);
	}

	// 2000-06-05 Changed
	final void writeLongInt(int i) throws SQLException {
		ensureCapacity(3);
		byte[] b = this.byteBuffer;
		b[this.position++] = (byte) (i & 0xff);
		b[this.position++] = (byte) (i >>> 8);
		b[this.position++] = (byte) (i >>> 16);
	}

	final void writeLongLong(long i) throws SQLException {
		ensureCapacity(8);
		byte[] b = this.byteBuffer;
		b[this.position++] = (byte) (i & 0xff);
		b[this.position++] = (byte) (i >>> 8);
		b[this.position++] = (byte) (i >>> 16);
		b[this.position++] = (byte) (i >>> 24);
		b[this.position++] = (byte) (i >>> 32);
		b[this.position++] = (byte) (i >>> 40);
		b[this.position++] = (byte) (i >>> 48);
		b[this.position++] = (byte) (i >>> 56);
	}

	// Write null-terminated string
	//modify in SQLMR
	final void writeString(String s) throws SQLException {
		ensureCapacity(s.getBytes().length);
		writeStringNoNull(s);
		//this.byteBuffer[this.position++] = 0;
	}
	
	//Using in ConnectSQLMR
	final void writeString(String s, String encoding) throws SQLException, UnsupportedEncodingException{
		ensureCapacity(s.getBytes().length);
		int len = s.getBytes(encoding).length;
		System.arraycopy(s.getBytes(encoding), 0, this.byteBuffer, this.position, len);
		this.position += len;
	}

	// Write string, with no termination
	//modify in SQLMR
	final void writeStringNoNull(String s) throws SQLException {
		int len = s.getBytes().length;
		ensureCapacity(len * 2);
		System.arraycopy(s.getBytes(), 0, this.byteBuffer, this.position, len);
		this.position += len;

	}

	// Write a String using the specified character
	// encoding

	//Create in SQLMR, writing statement length in Packet header
	final void writePacketLen() throws SQLException {
		ensureCapacity(2);

		byte[] b = this.byteBuffer;
		b[0] = (byte) ( (this.position - 3)& 0xff);//2 for packet length 1 for command
		b[1] = (byte) ( (this.position - 3)>>> 8);
	}
}
