/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestCallerContext {

  @Test
  public void testSetSyncFlag() {
    // Fist, old context is null.
    CallerContext.setSyncFlagForRBF(null);
    Assert.assertNotNull(CallerContext.getCurrent());
    Assert.assertEquals("syncFlag:needSyncObserverRead",
        CallerContext.getCurrent().getContext());
    Assert.assertNull(CallerContext.getCurrent().getSignature());

    // Second, old context is not null but invalid.
    CallerContext invalidContext = new CallerContext.Builder(null).build();
    CallerContext.setSyncFlagForRBF(invalidContext);
    Assert.assertNotNull(CallerContext.getCurrent());
    Assert.assertEquals("syncFlag:needSyncObserverRead",
        CallerContext.getCurrent().getContext());
    Assert.assertNull(CallerContext.getCurrent().getSignature());

    // Third, old context is valid but does not contain the sync flag.
    CallerContext validWithoutFlagContext = new CallerContext.Builder("hello word").build();
    CallerContext.setSyncFlagForRBF(validWithoutFlagContext);
    Assert.assertNotNull(CallerContext.getCurrent());
    Assert.assertTrue(CallerContext.getCurrent().getContext()
        .contains("syncFlag:needSyncObserverRead"));
    Assert.assertNull(CallerContext.getCurrent().getSignature());

    // Fourth, old context is valid and contains the sync flag.
    CallerContext validWithFlagContext = new CallerContext
        .Builder("hello word_needSyncObserverRead").build();
    CallerContext.setSyncFlagForRBF(validWithFlagContext);
    Assert.assertNotNull(CallerContext.getCurrent());
    Assert.assertTrue(CallerContext.getCurrent().getContext()
        .contains("syncFlag:needSyncObserverRead"));
    Assert.assertEquals(1, CallerContext.getCurrent().getContext()
        .split("needSyncObserverRead").length);
    Assert.assertNull(CallerContext.getCurrent().getSignature());

    // Five, old context is valid with one signature but without sync flag.
    byte[] signature = "L".getBytes(CallerContext.SIGNATURE_ENCODING);
    CallerContext validWithSignatureContext = new CallerContext
        .Builder("hello word").setSignature(signature).build();
    CallerContext.setSyncFlagForRBF(validWithSignatureContext);
    Assert.assertNotNull(CallerContext.getCurrent());
    Assert.assertTrue(CallerContext.getCurrent().getContext()
        .contains("syncFlag:needSyncObserverRead"));
    Assert.assertNotNull(CallerContext.getCurrent().getSignature());
    Assert.assertEquals(Arrays.toString(signature),
        Arrays.toString(CallerContext.getCurrent().getSignature()));
  }
}
