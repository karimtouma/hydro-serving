package io.hydrosphere.serving.manager.actor

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import envoy.api.v2.{AggregatedDiscoveryServiceGrpc, DiscoveryRequest, DiscoveryResponse}
import io.grpc.stub.StreamObserver
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import io.hydrosphere.serving.manager.service.envoy.xds.dto.{StartAck, StartMessage}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._


class FakeServer(factoryActor:ActorRef) extends AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryService {

  implicit val ctx = ExecutionContext.global

  var stream:StreamObserver[DiscoveryRequest] = _

  def sendNext(response: DiscoveryRequest): Unit = stream.onNext(response)
  def sendError(e: Throwable): Unit = stream.onError(e)
  def sendComplete(): Unit = stream.onCompleted()

  def ready():Future[Unit] = {
    if(stream != null) Future.successful(Unit)
    else {
      val block = Future{
        TimeUnit.SECONDS.sleep(1)
      }

      for{
        _ <- block
        result <- ready()
      } yield result
    }
  }

  override def streamAggregatedResources(responseObserver: StreamObserver[DiscoveryResponse]): StreamObserver[DiscoveryRequest] = {
    val timeout = 2 seconds
    implicit val implicitTimeout: Timeout = timeout
    val futureStream = factoryActor ? StartMessage(responseObserver)
    val ack = Await.result(futureStream.mapTo[StartAck], timeout)
    ack.resp
  }
}
