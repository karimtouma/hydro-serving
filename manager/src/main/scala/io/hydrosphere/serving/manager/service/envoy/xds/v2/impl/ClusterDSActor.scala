package io.hydrosphere.serving.manager.service.envoy.xds.v2.impl

import com.google.protobuf.any
import com.google.protobuf.duration.Duration
import envoy.api.v2.Cluster.EdsClusterConfig
import envoy.api.v2.ConfigSource.ConfigSourceSpecifier
import envoy.api.v2._
import io.hydrosphere.serving.manager.service.clouddriver.CloudDriverService
import io.hydrosphere.serving.manager.service.envoy.xds.dto._
import io.hydrosphere.serving.manager.service.envoy.xds.v2.{StateActor, XdsEventBus}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class ClusterDSActor(implicit requestBus:XdsEventBus[UpdateRequest, String], responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress])
  extends StateActor[ListBuffer[Cluster]](
  typeUrl = "type.googleapis.com/envoy.api.v2.Cluster"
) {

  private val clustersNames = new mutable.HashSet[String]()

  private val httClusters = Set(CloudDriverService.MANAGER_HTTP_NAME)

  override def initialState(): ListBuffer[Cluster] = ListBuffer()

  override def resources(state: ListBuffer[Cluster]): Seq[any.Any] = state.map(any.Any.pack(_))

  override def receiveStoreChangeEvents(mes: Any): Boolean = {
    val results = mes match {
      case AddCluster(names) =>
        addClusters(names)
      case RemoveClusters(names) =>
        removeClusters(names)
      case SyncCluster(names) =>
        syncClusters(names)
    }
    results.contains(true)
  }

  private def createCluster(name: String): Cluster = {
    val res = Cluster(
      name = name,
      `type` = Cluster.DiscoveryType.EDS,
      connectTimeout = Some(Duration(seconds = 0, nanos = 25000000)),
      edsClusterConfig = Some(
        EdsClusterConfig(
          edsConfig = Some(ConfigSource(
            configSourceSpecifier = ConfigSourceSpecifier.Ads(
              AggregatedConfigSource()
            ))
          )
        )
      )
    )

    if (httClusters.contains(name)) {
      res
    } else {
      res.withHttp2ProtocolOptions(Http2ProtocolOptions())
    }
  }

  private def addClusters(names: Set[String]): Set[Boolean] =
    names.map(name => {
      if (clustersNames.add(name)) {
        state += createCluster(name)
        true
      } else {
        false
      }
    })

  private def removeClusters(names: Set[String]): Set[Boolean] =
    names.map(name => {
      if (clustersNames.remove(name)) {
        state --= state.filter(c => !clustersNames.contains(c.name))
        true
      } else {
        false
      }
    })


  private def syncClusters(names: Set[String]): Set[Boolean] = {
    val toRemove = clustersNames.toSet -- names
    val toAdd = names -- clustersNames

    removeClusters(toRemove) ++ addClusters(toAdd)
  }


}
