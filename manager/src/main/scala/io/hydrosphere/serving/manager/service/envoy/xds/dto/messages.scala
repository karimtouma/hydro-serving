package io.hydrosphere.serving.manager.service.envoy.xds.dto

import akka.actor.ActorRef
import envoy.api.v2.{DiscoveryRequest, DiscoveryResponse, Node}
import io.grpc.stub.StreamObserver
import io.hydrosphere.serving.manager.model.{Application, Service}
import io.hydrosphere.serving.manager.service.clouddriver.CloudService

case class AddCluster(names: Set[String])

case class RemoveClusters(names: Set[String])

case class SyncCluster(names: Set[String])

case class SyncApplications(applications: Seq[Application])

case class ApplicationChanged(application: Application)

case class ApplicationRemoved(application: Application)

case class ServiceChanged(service: Service)

case class ServiceRemoved(service: Service)

case class CloudServiceDetected(cloudServices: Seq[CloudService])

case class RenewEndpoints(clusters: Seq[ClusterInfo])

case class AddEndpoints(clusters: Seq[ClusterInfo])

case class RemoveEndpoints(names: Set[String])

case class SubscribeMsg(discoveryRequest: DiscoveryRequest, responseObserver: StreamObserver[DiscoveryResponse])

case class UnsubscribeMsg(responseObserver: StreamObserver[DiscoveryResponse])

case class GetNodes()

case class UpdateRequest(request:DiscoveryRequest, node:Option[Node] = None)

case class UpdateResponse(response:DiscoveryResponse, node:Option[Node] = None)

case class StartMessage(stream:StreamObserver[DiscoveryResponse])

case class StartAck(resp:StreamObserver[DiscoveryRequest])

case class NodesReport()

