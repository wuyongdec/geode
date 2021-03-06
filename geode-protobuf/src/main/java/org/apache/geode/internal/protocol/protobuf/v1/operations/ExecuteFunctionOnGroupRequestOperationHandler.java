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
package org.apache.geode.internal.protocol.protobuf.v1.operations;

import java.util.List;
import java.util.Set;

import com.google.protobuf.ProtocolStringList;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.exception.InvalidExecutionContextException;
import org.apache.geode.internal.protocol.operations.ProtobufOperationHandler;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.Failure;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnGroupRequest;
import org.apache.geode.internal.protocol.protobuf.v1.FunctionAPI.ExecuteFunctionOnGroupResponse;
import org.apache.geode.internal.protocol.protobuf.v1.MessageExecutionContext;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.Result;
import org.apache.geode.internal.protocol.protobuf.v1.Success;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;

public class ExecuteFunctionOnGroupRequestOperationHandler implements
    ProtobufOperationHandler<ExecuteFunctionOnGroupRequest, ExecuteFunctionOnGroupResponse> {

  @Override
  public Result<FunctionAPI.ExecuteFunctionOnGroupResponse> process(
      ProtobufSerializationService serializationService,
      FunctionAPI.ExecuteFunctionOnGroupRequest request,
      MessageExecutionContext messageExecutionContext)
      throws InvalidExecutionContextException, DecodingException, EncodingException {

    final String functionID = request.getFunctionID();
    List<String> memberNameList = request.getGroupNameList();
    Object arguments = getFunctionArguments(request, serializationService);

    List<Object> results = messageExecutionContext.getAuthorizingCache().getFunctionService()
        .executeFunctionOnGroups(functionID, arguments, memberNameList);

    final FunctionAPI.ExecuteFunctionOnGroupResponse.Builder responseMessage =
        FunctionAPI.ExecuteFunctionOnGroupResponse.newBuilder();

    results.stream().map(serializationService::encode).forEach(responseMessage::addResults);

    return Success.of(responseMessage.build());
  }

  private Object getFunctionArguments(ExecuteFunctionOnGroupRequest request,
      ProtobufSerializationService serializationService) throws DecodingException {
    if (request.hasArguments()) {
      return serializationService.decode(request.getArguments());
    } else {
      return null;
    }
  }

}
