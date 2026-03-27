namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests

module PulsarClientBuilderTests =

    type private ManualServiceInfoProvider(initial: ServiceInfo) =
        inherit ServiceInfoProvider()

        let mutable onUpdate = ignore

        member _.Update(serviceInfo: ServiceInfo) =
            onUpdate serviceInfo

        override _.InitialServiceInfo() = initial

        override _.Initialize(callback) =
            onUpdate <- callback

    let private builder() =
        PulsarClientBuilder()

    let configure builderF builder =
        fun() ->  builder |> builderF |> ignore

    [<Tests>]
    let tests =

        testList "PulsarClientBuilderTests" [

            test "WithServiceUrl throws an exception for blank url" {
                let checkUrl url =
                    builder()
                    |> configure(fun b -> b.ServiceUrl url)
                    |> Expect.throwsWithMessage<ArgumentException> "ServiceUrl must not be blank."
                [null; ""; " "] |> List.iter checkUrl
            }

            test "MaxNumberOfRejectedRequestPerConnection throws an exception for negative value" {
                builder()
                |> configure(fun b -> b.MaxNumberOfRejectedRequestPerConnection -1)
                |> Expect.throwsWithMessage<ArgumentException> "MaxNumberOfRejectedRequestPerConnection can't be negative"
            }

            test "Build throws an exception if ServiceUrl is empty" {
                fun() -> builder().BuildAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "ServiceUrl or ServiceInfoProvider needs to be specified on the PulsarClientBuilder object. (Parameter 'config')"
            }

            testTask "Build works with ServiceInfoProvider only" {
                let provider = ManualServiceInfoProvider(ServiceInfo("pulsar://localhost:6650"))
                let! (client: PulsarClient) = builder().ServiceInfoProvider(provider).BuildAsync()
                client.ServiceInfo.ServiceUrl |> Expect.equal "" "pulsar://localhost:6650"
                do! client.CloseAsync()
            }

            test "Build throws when ServiceUrl and ServiceInfoProvider are configured together" {
                let provider = ManualServiceInfoProvider(ServiceInfo("pulsar://localhost:6650"))
                fun() -> builder().ServiceUrl("pulsar://localhost:6650").ServiceInfoProvider(provider).BuildAsync() |> ignore
                |> Expect.throwsWithMessage<ArgumentException>
                    "ServiceUrl and ServiceInfoProvider cannot be configured together. (Parameter 'config')"
            }

            test "Http lookup authentication authDataProvider" {
                AuthenticationFactory.Token("test").GetAuthData().HasDataForHttp()
                |> Expect.equal "AuthenticationToken HasDataForHttp should be true" true

                AuthenticationFactoryOAuth2.ClientCredentials(
                    Uri("https://test.com"),
                    "test",
                    Uri("https://test.com")
                ).GetAuthMethodName()
                |> Expect.equal "AuthenticationFactoryOAuth2 authData should be token" "token"
            }


        ]
