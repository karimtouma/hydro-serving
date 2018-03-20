package io.hydrosphere.serving.manager.service.envoy.xds.v2.impl

import com.google.protobuf.any
import envoy.api.v2.RouteAction.ClusterSpecifier
import envoy.api.v2._
import io.hydrosphere.serving.manager.model.Application
import io.hydrosphere.serving.manager.service.clouddriver.CloudDriverService
import io.hydrosphere.serving.manager.service.envoy.xds.dto._
import io.hydrosphere.serving.manager.service.envoy.xds.v2.{StateActor, XdsEventBus}

import scala.collection.mutable

class RouteDSActor(implicit requestBus:XdsEventBus[UpdateRequest, String], responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress])
  extends StateActor[mutable.Map[Long, Seq[VirtualHost]]](typeUrl = "type.googleapis.com/envoy.api.v2.RouteConfiguration"){

  override def initialState(): mutable.Map[Long, Seq[VirtualHost]] = mutable.Map[Long, Seq[VirtualHost]]()

  private val kafkaGatewayHost=createGatewayHost(CloudDriverService.GATEWAY_KAFKA_NAME)

  override def resources(state: mutable.Map[Long, Seq[VirtualHost]]): Seq[any.Any] = {
    //val clusterName = getObserverNode(responseObserver).fold("manager_xds_cluster")(_.id)

//    val defaultRoute = clusterName match {
//      case CloudDriverService.MANAGER_NAME =>
//        defaultManagerVirtualHost()
//      case _ =>
//        defaultVirtualHost(clusterName)
//    }
//
//    Seq(createRoute(ROUTE_CONFIG_NAME, defaultRoute)).map(any.Any.pack(_))
    Seq()
  }

  override def receiveStoreChangeEvents(mes: Any): Boolean =
    mes match {
      case a: SyncApplications =>
        renewApplications(a.applications)
        true
      case a: ApplicationChanged =>
        addOrUpdateApplication(a.application)
        true
      case a: ApplicationRemoved =>
        removeApplications(Set(a.application.id))
          .contains(true)
      case _ => false
    }

  private def createRoutes(application: Application): Seq[VirtualHost] =
    application.executionGraph.stages.zipWithIndex.map { case (appStage, i) =>
      val weights = ClusterSpecifier.WeightedClusters(WeightedCluster(
        clusters = appStage.services.map(w => {
          WeightedCluster.ClusterWeight(
            name = w.serviceDescription.toServiceName(),
            weight = Some(w.weight)
          )
        })
      ))
      //TODO generate unique ID
      createVirtualHost(s"app${application.id}stage$i", weights)
    }

  private def createVirtualHost(name: String, weights: ClusterSpecifier.WeightedClusters): VirtualHost =
    VirtualHost(
      name = name,
      domains = Seq(name),
      routes = Seq(Route(
        `match` = Some(RouteMatch(
          pathSpecifier = RouteMatch.PathSpecifier.Prefix("/")
        )),
        action = Route.Action.Route(RouteAction(
          clusterSpecifier = weights
        ))
      ))
    )

  private def addOrUpdateApplication(application: Application): Unit =
    state.put(application.id, createRoutes(application))


  private def removeApplications(ids: Set[Long]): Set[Boolean] =
    ids.map(id => state.remove(id).nonEmpty)


  private def renewApplications(apps: Seq[Application]): Unit = {
    state.clear()
    apps.foreach(addOrUpdateApplication)
  }



  private def createRoute(name: String, defaultRoute: VirtualHost): RouteConfiguration =
    RouteConfiguration(
      name = name,
      virtualHosts = state.values.flatten.toSeq :+ defaultRoute :+ kafkaGatewayHost
    )

  private def createGatewayHost(name: String): VirtualHost =
    VirtualHost(
      name = name,
      domains = Seq(name),
      routes = Seq(Route(
        `match` = Some(RouteMatch(
          pathSpecifier = RouteMatch.PathSpecifier.Prefix("/")
        )),
        action = Route.Action.Route(RouteAction(
          clusterSpecifier = ClusterSpecifier.Cluster(name)
        ))
      ))
    )

  private def defaultVirtualHost(clusterName: String): VirtualHost =
    VirtualHost(
      name = "all",
      domains = Seq("*"),
      routes = Seq(Route(
        `match` = Some(RouteMatch(
          pathSpecifier = RouteMatch.PathSpecifier.Prefix("/")
        )),
        action = Route.Action.Route(RouteAction(
          clusterSpecifier = ClusterSpecifier.Cluster(clusterName)
        ))
      ))
    )

  private def defaultManagerVirtualHost(): VirtualHost =
    VirtualHost(
      name = "all",
      domains = Seq("*"),
      routes = Seq(
        Route(
          `match` = Some(RouteMatch(
            pathSpecifier = RouteMatch.PathSpecifier.Prefix("/"),
            headers = Seq(HeaderMatcher(
              name = "content-type",
              value = "application/grpc"
            ))
          )),
          action = Route.Action.Route(RouteAction(
            clusterSpecifier = ClusterSpecifier.Cluster(CloudDriverService.MANAGER_NAME)
          ))
        ),
        Route(
          `match` = Some(RouteMatch(
            pathSpecifier = RouteMatch.PathSpecifier.Prefix("/")
          )),
          action = Route.Action.Route(RouteAction(
            clusterSpecifier = ClusterSpecifier.Cluster(CloudDriverService.MANAGER_HTTP_NAME)
          ))
        )
      )
    )

}
