package io.hydrosphere.serving.manager.service.envoy.xds.v2.impl

import com.google.protobuf.any
import envoy.api.v2.DiscoveryResponse
import io.hydrosphere.serving.manager.grpc.applications.{Application, ExecutionGraph, ExecutionStage, KafkaStreaming}
import io.hydrosphere.serving.manager.model.ApplicationStage
import io.hydrosphere.serving.manager.service.envoy.xds.dto._
import io.hydrosphere.serving.manager.service.envoy.xds.v2.{StateActor, XdsEventBus}

import scala.collection.mutable

class ApplicationDSActor(implicit requestBus:XdsEventBus[UpdateRequest, String], responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress])
  extends StateActor[mutable.Map[Long, Application]](
    typeUrl = "type.googleapis.com/io.hydrosphere.serving.manager.grpc.applications.Application"
  ){


  override def initialState(): mutable.Map[Long, Application] = mutable.Map[Long, Application]()

  override def resources(state: mutable.Map[Long, Application]): Seq[any.Any] = state.values.map(any.Any.pack(_))toSeq

  override def receiveStoreChangeEvents(mes: Any): Boolean = mes match {
    case a: SyncApplications =>
      state.clear()
      addOrUpdateApplications(a.applications)
      true
    case a: ApplicationChanged =>
      addOrUpdateApplications(Seq(a.application))
      true
    case a: ApplicationRemoved =>
      removeApplications(Set(a.application.id))
        .contains(true)
    case _ => false
  }

  private def addOrUpdateApplications(apps: Seq[io.hydrosphere.serving.manager.model.Application]): Unit =
    apps.map(p => Application(
      id = p.id,
      name = p.name,
      contract = Option(p.contract),
      executionGraph = Option(ExecutionGraph(
        p.executionGraph.stages.zipWithIndex.map {
          case (stage, idx) => ExecutionStage(
            stageId = ApplicationStage.stageId(p.id, idx),
            signature = stage.signature
          )
        }
      )),
      kafkaStreaming = p.kafkaStreaming.map(k => KafkaStreaming(
        consumerId = k.consumerId.getOrElse(s"appConsumer${p.id}"),
        sourceTopic = k.sourceTopic,
        destinationTopic = k.destinationTopic,
        errorTopic = k.errorTopic.getOrElse("")
      ))
    )).foreach(a => {
      state.put(a.id, a)
    })

  private def removeApplications(ids: Set[Long]): Set[Boolean] =
    ids.map(id => state.remove(id).nonEmpty)
}
