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

package org.apache.kyuubi.server.api.v1

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag

import org.apache.kyuubi.Logging
import org.apache.kyuubi.ha.client.DiscoveryClientProvider
import org.apache.kyuubi.server.api.ApiRequestContext
import org.apache.kyuubi.session.KyuubiSessionManager

@Tag(name = "ServiceRegistry")
@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ServiceRegistryResource extends ApiRequestContext with Logging {

  private def sessionManager = fe.be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private def conf = sessionManager.getConf

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[CreatePathResponse]))),
    description = "create a path")
  @POST
  @Path("/createPath")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createPath(request: CreatePathRequest): CreatePathResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.create(request.path, request.mode, request.createParent)
      }
    CreatePathResponse(result)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetPathDataResponse]))),
    description = "get path data")
  @GET
  @Path("/getPathData")
  def getPathData(@QueryParam("path") path: String): GetPathDataResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.getData(path)
      }
    GetPathDataResponse(result)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetPathChildrenResponse]))),
    description = "get path children")
  @GET
  @Path("/getPathChildren")
  def getPathChildren(@QueryParam("path") path: String): GetPathChildrenResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.getChildren(path)
      }
    GetPathChildrenResponse(result.toArray)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetPathExistsResponse]))),
    description = "get whether path exits")
  @GET
  @Path("/getPathExists")
  def getPathExists(@QueryParam("path") path: String): GetPathExistsResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.pathExists(path)
      }
    GetPathExistsResponse(result)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[DeletePathResponse]))),
    description = "delete path")
  @GET
  @Path("/deletePath")
  def deletePath(@QueryParam("path") path: String,
                 @QueryParam("deleteChildren") deleteChildren: Boolean): DeletePathResponse = {
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.delete(path, deleteChildren)
      }
    DeletePathResponse()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetServerHostResponse]))),
    description = "get server host and port")
  @GET
  @Path("/getServerHost")
  def getServerHost(@QueryParam("namespace") namespace: String): GetServerHostResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.getServerHost(namespace)
      }
    if (result.isEmpty) {
      GetServerHostResponse(null, 0)
    } else {
      GetServerHostResponse(result.get._1, result.get._2)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetEngineByRefIdResponse]))),
    description = "get engine host and port")
  @GET
  @Path("/getEngineByRefId")
  def getEngineByRefId(@QueryParam("namespace") namespace: String,
                       @QueryParam("engineRefId") engineRefId: String): GetEngineByRefIdResponse = {
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.getEngineByRefId(namespace, engineRefId)
      }
    if (result.isEmpty) {
      GetEngineByRefIdResponse(null, 0)
    } else {
      GetEngineByRefIdResponse(result.get._1, result.get._2)
    }
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[GetServiceNodesInfoResponse]))),
    description = "get service nodes information")
  @GET
  @Path("/getServiceNodesInfo")
  def getServiceNodesInfo(@QueryParam("namespace") namespace: String,
                          @QueryParam("sizeOpt") size: Int = 0,
                        @QueryParam("silent") silent: Boolean = false):
    GetServiceNodesInfoResponse = {
    val sizeOpt = if (size <= 0) {
      None
    } else {
      Some(size)
    }
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.getServiceNodesInfo(namespace, sizeOpt, silent)
      }
    GetServiceNodesInfoResponse(
      result.map(GetServiceNodesInfoResponseServiceNodeInfo.convert(_)).toArray)
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[RegisterServiceResponse]))),
    description = "Register service")
  @POST
  @Path("/registerService")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def registerService(request: RegisterServiceRequest): RegisterServiceResponse = {
    info(s"Registering external service: $request")
    val refId = Option(request.refId)
    val version = Option(request.version)
    DiscoveryClientProvider.withDiscoveryClient(conf) {
      c => c.registerExternalService(conf,
        request.namespace,
        request.connectionUrl,
        refId,
        version)
    }
    RegisterServiceResponse()
  }

  @ApiResponse(
    responseCode = "200",
    content = Array(new Content(
      mediaType = MediaType.APPLICATION_JSON,
      schema = new Schema(implementation = classOf[CreateAndGetServiceNodeResponse]))),
    description = "Register service")
  @POST
  @Path("/createAndGetServiceNode")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def createAndGetServiceNode(request: CreateAndGetServiceNodeRequest):
    CreateAndGetServiceNodeResponse = {
    val refId = Option(request.refId)
    val version = Option(request.version)
    val result =
      DiscoveryClientProvider.withDiscoveryClient(conf) {
        c => c.createAndGetServiceNode(conf,
          request.namespace,
          request.instance,
          refId,
          version,
          request.external)
      }
    CreateAndGetServiceNodeResponse(result)
  }
}


