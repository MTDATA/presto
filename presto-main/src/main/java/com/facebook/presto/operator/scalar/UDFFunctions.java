/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class UDFFunctions {

    private final static Slice DASH = Slices.wrappedBuffer("-".getBytes());
    private final static Slice EMPTY = Slices.wrappedBuffer("".getBytes());
    private final static Slice UTF8 = Slices.wrappedBuffer("utf-8".getBytes());

    public UDFFunctions() {

    }

    @Description("Cast datekey to date, or otherwise datekey to date")
    @ScalarFunction
    public static Slice d2d(final Slice s, final Slice sourceSep, final Slice targetSep) {
        if (s == null) {
            return null;
        }
        try {
            Date d = new SimpleDateFormat("yyyy" + sourceSep.toStringUtf8() + "MM" + sourceSep.toStringUtf8() + "dd").parse(s.toStringUtf8());
            return Slices.wrappedBuffer(new SimpleDateFormat("yyyy" + targetSep.toStringUtf8() + "MM" + targetSep.toStringUtf8() + "dd").format(d).getBytes());
        } catch (Exception ex) {
            return null;
        }
    }

    @Description("Cast date to datekey, eg: 2012-08-23 -> 20120823")
    @ScalarFunction
    public static Slice date2datekey(Slice date) {
        return d2d(date, DASH, EMPTY);
    }

    @Description("Cast datekey to date, eg: 20120823 -> 2012-08-23")
    @ScalarFunction
    public static Slice datekey2date(Slice datekey) {
        return d2d(datekey, EMPTY, DASH);
    }

    @Description("Return a substring from a string before the specified number of occurrences of the delimiter " +
            "\neg: SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);\n" + "  -> 'www.mysql'")
    @ScalarFunction
    public static Slice substring_index(Slice input, Slice delim, long pos) {
        if (input == null || delim == null) {
            return null;
        }

        String inputStr = input.toStringUtf8(), delimStr = delim.toStringUtf8();
        if (pos == 0 || "".equals(delimStr)) {
            return input;
        }

        int count = 0;

        if (pos > 0) {
            int k = -1;

            while (count++ < pos) {
                k = inputStr.indexOf(delimStr, k + 1);
                if (k < 0) {
                    return input;
                }
            }
            return Slices.wrappedBuffer(inputStr.substring(0, k).getBytes());
        }

        int k = inputStr.length() + 1;

        while (count-- > pos) {
            k = inputStr.lastIndexOf(delimStr, k - 1);
            if (k < 0) {
                return input;
            }
        }

        return Slices.wrappedBuffer(inputStr.substring(k + delimStr.length()).getBytes());
    }

    @Description("Calculate md5 of the string")
    @ScalarFunction
    public static Slice md5(final Slice s) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        if (s == null) {
            return null;
        }
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(s.toStringUtf8().getBytes("UTF8"));
        byte[] md5hash = md.digest();
        StringBuilder builder = new StringBuilder();
        for (byte b : md5hash) {
            builder.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        }
        return Slices.wrappedBuffer(builder.toString().getBytes());
    }

    @Description("Desc 'abc%20def' -> 'abc def'")
    @ScalarFunction
    public static Slice urldecode(final Slice key) {
        return urldecode(key, UTF8);
    }

    @Description("Desc 'abc%20def' -> 'abc def'")
    @ScalarFunction
    public static Slice urldecode(final Slice key, final Slice enc) {
        try {
            return Slices.wrappedBuffer(URLDecoder.decode(key.toStringUtf8(), enc.toStringUtf8()).getBytes());
        } catch (Exception e) {
            return EMPTY;
        }
    }

//    @Description("Return the largest argument")
//    @ScalarFunction
//    public static long greatest(long...values) {
//        long greatest = values[0];
//        for (int i = 1; i < values.length; i++) {
//            greatest = Math.max(greatest, values[i]);
//        }
//        return greatest;
//    }
//
//    @Description("Return the largest argument")
//    @ScalarFunction
//    public static double greatest(double...values) {
//        double greatest = values[0];
//        for (int i = 1; i < values.length; i++) {
//            greatest = Math.max(greatest, values[i]);
//        }
//        return greatest;
//    }
//
//    @Description("Return the smallest argument")
//    @ScalarFunction
//    public static long least(long...values) {
//        long least = values[0];
//        for (int i = 1; i < values.length; i++) {
//            least = Math.min(least, values[i]);
//        }
//        return least;
//    }
//
//    @Description("Return the smallest argument")
//    @ScalarFunction
//    public static double least(double...values) {
//        double least = values[0];
//        for (int i = 1; i < values.length; i++) {
//            least = Math.min(least, values[i]);
//        }
//        return least;
//    }

}
