/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

//@RunWith(ConcurrentTestRunner.class)
//@Category(IntegrationTest.class)
public class FilterProfileConcurrencyTest {

//  @Test
//  public void serializationOfFilterProfileWithConcurrentUpdateShouldSucceed(
//      ParallelExecutor executor) throws Exception {
//    // warmUp();
//
//    FilterProfile profile = createFilterProfile();
//
//    // In parallel, serialize the filter profile
//    // and add a new client
//    Future<byte[]> serializer = executor.inParallel(() -> serialize(profile));
//    executor.inParallel(() -> addClient(profile));
//    executor.execute();
//
//    // Make sure we can deserialize the filter profile
//    byte[] bytes = serializer.get();
//    Object deserialized = deserialize(bytes);
//    assertEquals(FilterProfile.class, deserialized.getClass());
//
//  }
//
//  private Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
//    return BlobHelper.deserializeBlob(bytes, Version.CURRENT, null);
//  }
//
//  private FilterProfile createFilterProfile() throws UnknownHostException {
//    DistributedMember member = new InternalDistributedMember(InetAddress.getLocalHost(), 0, false,
//        false, MemberAttributes.DEFAULT);
//    return new FilterProfile(null, member, true);
//  }
//
//  private Set addClient(FilterProfile profile) {
//    return profile.registerClientInterest("client", ".*", InterestType.REGULAR_EXPRESSION, false);
//  }
//
//  private byte[] serialize(FilterProfile profile) throws IOException {
//    return BlobHelper.serializeToBlob(profile);
//  }
//
//  private void warmUp() throws IOException {
//    FilterProfile profile = createFilterProfile();
//    byte[] bytes = BlobHelper.serializeToBlob(profile);
//    addClient(profile);
//  }
}
