package io.hydrosphere.serving.manager.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.scala.Logging
import org.scalatest._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import envoy.api.v2.{AggregatedDiscoveryServiceGrpc, DiscoveryRequest, DiscoveryResponse, Node}
import io.grpc.{ConnectivityState, ManagedChannel, ManagedChannelBuilder, Server}
import io.grpc.stub.StreamObserver
import com.google.protobuf.any
import io.hydrosphere.serving.manager.grpc.applications.Application
import io.hydrosphere.serving.manager.service.envoy.xds.dto.{GetNodes, SubscriptionAddress, UpdateRequest, UpdateResponse}
import io.hydrosphere.serving.manager.service.envoy.xds.v2._

import scala.concurrent.duration._
import scala.concurrent._

class StateStreamingSpecs extends TestKit(
  ActorSystem(
  "TestKitUsageSpec",
  ConfigFactory.parseString(AkkaConfig.config))
) with DefaultTimeout
  with GivenWhenThen
  with ImplicitSender
  with Matchers
  with WordSpecLike
  with BeforeAndAfterAll
  with Logging{

  var actorFactory:ActorRef = _
  var requestBus: XdsEventBus[UpdateRequest, String] = _
  var responseBus:XdsEventBus[UpdateResponse, SubscriptionAddress] = _
  val port = 9487
  var service:FakeServer = _
  var server:Server = _
  var managedChannel:ManagedChannel = _
  var client:AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub = _
  private val testTypeUrl = "testTypeUrl"

  val stream = new TestStream


  override def afterAll(): Unit = {
    super.afterAll()

    server.shutdownNow()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    requestBus = new XdsRequestEventBus()
    responseBus = new XdsResponseEventBus()

    actorFactory = system.actorOf(Props(classOf[SubscriptionFactoryActor], requestBus, responseBus), "subscriptions")

    service = new FakeServer(actorFactory)
    server = io.grpc.ServerBuilder.forPort(port).addService(AggregatedDiscoveryServiceGrpc.bindService(service, ExecutionContext.global)).build()

    server.start()

    managedChannel = ManagedChannelBuilder
      .forAddress("localhost", port)
      .usePlaintext(true)
      .build

    while (managedChannel.getState(true) != ConnectivityState.READY){
      TimeUnit.SECONDS.sleep(1)
      logger.info(s"waiting for server connection. Current state is ${managedChannel.getState(true)}")
    }

    client = AggregatedDiscoveryServiceGrpc.stub(managedChannel)



  }

  "state streaming system" should {

    "create  subscription actor for every request" in {
      When("creating new servier connection")
      val requestListener = system.actorOf(Props(new RequestListenerActor()))
      val responseListener = system.actorOf(Props(new ResponseListenerActor()))
      val stateActor = system.actorOf(Props(new ListIntStateActor()))

      val futureResponce = stream.nextMessage()
      stateActor ! StateUpdate(List(Application(1)))

      val node = Node("1", "cluster")

      val request = new DiscoveryRequest(
        versionInfo = "0",
        node = Some(node),
        typeUrl =  testTypeUrl
      )

      requestBus.subscribe(stateActor, testTypeUrl)
      responseBus.subscribe(responseListener, SubscriptionAddress(testTypeUrl))
      responseBus.subscribe(responseListener, SubscriptionAddress(testTypeUrl, Some(node)))

      val in = client.streamAggregatedResources(stream)

      in.onNext(request)
      receiveWhile[DiscoveryRequest](500 millis){
        case msg:DiscoveryRequest => msg
      }

      Then("request message published to request event bus")
      val updateRequests = within(1 second){
        requestListener ! GetState()
        expectMsgAnyClassOf(1 second, classOf[List[UpdateRequest]])
      }
      updateRequests.isEmpty shouldBe(false)

      And("node connection should be up")
      val futureNodes = within(2 second){
        actorFactory ! GetNodes()
        expectMsg(Set(request.node.get))
      }

      val newState = within(1 second){
        stateActor ! GetState
        expectMsgAnyClassOf(1 second, classOf[List[Application]])
      }

      Then("request message published to request event bus")
      val updateResponses = within(1 second){
        responseListener ! GetState()
        expectMsgAnyClassOf(1 second, classOf[List[DiscoveryResponse]])
      }

      And("subscribed grpc client should receive discovery response with new state")
      val responseWithNewState = Await.result(futureResponce, 2 second)
      updateResponses.head shouldBe(responseWithNewState)

      When("grpc client sends ack about update")

      val thisMassageShouldFail = stream.nextMessage()

      in.onNext(DiscoveryRequest(
        typeUrl = responseWithNewState.typeUrl,
        responseNonce = responseWithNewState.nonce,
        versionInfo = responseWithNewState.versionInfo
      ))

      Then("new update shouldn't be send since version is up to date")

      val thrown = the [java.util.concurrent.TimeoutException] thrownBy Await.result(thisMassageShouldFail, 2 seconds)

      thrown.getMessage shouldBe("Futures timed out after [2 seconds]")

    }

  }

  case class StateUpdate(state:List[Application])

  class ListIntStateActor extends {
  } with StateActor[List[Application]](
    typeUrl = testTypeUrl
  )(    requestBus = requestBus,
    responseBus = responseBus) {
    override def initialState(): List[Application] = List()

    def testHandler: Receive = {
      case a @ GetState => {
        sender() ! state
      }
    }

    override def receive: Receive = testHandler orElse super.receive

    override def receiveStoreChangeEvents(mes: Any): Boolean = mes match {
      case StateUpdate(newState) => {
        state = newState
        true
      }
    }

    override def resources(state: List[Application]): Seq[any.Any] = state.map(
      com.google.protobuf.any.Any.pack(_)
    )
  }

  case class GetState()

  class ResponseListenerActor extends Actor with ActorLogging {

    var updates:List[DiscoveryResponse] = List()
    responseBus.subscribe(self, SubscriptionAddress(testTypeUrl))

    override def receive: Receive = {
      case m: UpdateResponse => {
        updates = m.response :: updates
      }
      case ask:GetState => {
        sender() ! updates
      }
      case _ => {
        log.error("unsupported message")
      }
    }
  }

  class RequestListenerActor extends Actor with ActorLogging {

    var updates:List[UpdateRequest] = List()

    requestBus.subscribe(self, testTypeUrl)

    override def receive: Receive = {
      case m: UpdateRequest => {
        updates = m :: updates
      }
      case ask:GetState => {
        sender() ! updates
      }
      case _ => {
        log.error("unsupported message")
      }
    }
  }

  class TestStream extends StreamObserver[DiscoveryResponse] {

    var subscriptions = List[Promise[DiscoveryResponse]]()

    def nextMessage():Future[DiscoveryResponse] = {
      val promise = Promise[DiscoveryResponse]
      synchronized{
        subscriptions = promise::subscriptions
      }
      promise.future
    }

    override def onError(t: Throwable): Unit = {}
    override def onCompleted(): Unit = {}
    override def onNext(value: DiscoveryResponse): Unit = {
      synchronized{
        subscriptions.foreach(_.success(value))
        subscriptions = List[Promise[DiscoveryResponse]]()
      }
    }
  }


}



