package io.hydrosphere.serving.manager.service.envoy.xds.dto

import envoy.api.v2.Node

case class ClusterEndpoint(host: String, port: Int)

case class ClusterInfo(name: String, endpoints: Set[ClusterEndpoint])

case class VersionInfo(stateVersion:String, nodeVersion:String)

case class SubscriptionAddress(typeUrl:String, node:Option[Node] = None)
