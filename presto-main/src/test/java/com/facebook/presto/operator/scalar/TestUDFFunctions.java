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

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestUDFFunctions {

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testd2d()
    {
        assertFunction("d2d('2013-09-02', '-', '')", "20130902");
    }

    @Test
    public void testdate2datekey()
    {
        assertFunction("date2datekey('2013-09-02')", "20130902");
    }

    @Test
    public void testdatekey2date()
    {
        assertFunction("datekey2date('20130902')", "2013-09-02");
    }

    @Test
    public void testsubstring_index()
    {
        assertFunction("substring_index('www.mysql.com', '.', 2)", "www.mysql");
    }

    @Test
    public void testmd5()
    {
        assertFunction("md5('presto')", "1e954771813210da5673f51da7e4394c");
    }

    @Test
    public void testurldecode()
    {
        assertFunction("urldecode('abc%20def')", "abc def");
    }

//    @Test
//    public void testgreatest()
//    {
//        assertFunction("greatest(3, 2, 10, 9)", "10");
//        assertFunction("greatest(3.0, 2.1, 10, 9.1)", "10");
//    }
//
//    @Test
//    public void testleast()
//    {
//        assertFunction("least(3, 2, 10, 9)", "2");
//        assertFunction("least(3.0, 2.1, 10, 9.1)", "2.1");
//    }
}
