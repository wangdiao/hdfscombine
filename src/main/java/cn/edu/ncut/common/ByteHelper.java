package cn.edu.ncut.common;

public class ByteHelper {

	public static byte[] i2b(int i) {
		return new byte[] { (byte) ((i >> 24) & 0xFF),
				(byte) ((i >> 16) & 0xFF), (byte) ((i >> 8) & 0xFF),
				(byte) (i & 0xFF) };
	}

	public static int b2i(byte[] b) {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (b[i] & 0x000000FF) << shift;
		}
		return value;
	}

//	public static long bytes2long(byte[] b) {
//
//		int mask = 0xff;
//		int temp = 0;
//		int res = 0;
//		for (int i = 0; i < 8; i++) {
//			res <<= 8;
//			temp = b[i] & mask;
//			res |= temp;
//		}
//		return res;
//	}
//
//	public static byte[] long2bytes(long num) {
//		byte[] b = new byte[8];
//		for (int i = 0; i < 8; i++) {
//			b[i] = (byte) (num >>> (56 - i * 8));
//		}
//		return b;
//	}

}
