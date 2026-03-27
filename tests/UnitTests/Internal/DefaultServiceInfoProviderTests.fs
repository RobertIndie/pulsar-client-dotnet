namespace Pulsar.Client.UnitTests.Internal

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.Internal
open Pulsar.Client.UnitTests

module DefaultServiceInfoProviderTests =

    type private ManualServiceInfoProvider(initial: ServiceInfo) =
        inherit ServiceInfoProvider()

        let mutable onUpdate = ignore

        member _.Update(serviceInfo: ServiceInfo) =
            onUpdate serviceInfo

        override _.InitialServiceInfo() = initial

        override _.Initialize(callback) =
            onUpdate <- callback

    [<Tests>]
    let tests =
        testList "DefaultServiceInfoProviderTests" [
            test "Default provider maps config to ServiceInfo" {
                let config =
                    { PulsarClientConfiguration.Default with
                        ServiceUrl = "pulsar://localhost:6650"
                        UseTls = true
                        Authentication = AuthenticationFactory.Token("token") }

                let provider = DefaultServiceInfoProvider(config)
                let serviceInfo = provider.InitialServiceInfo()

                serviceInfo.ServiceUrl |> Expect.equal "" "pulsar://localhost:6650"
                serviceInfo.UseTls |> Expect.equal "" true
                serviceInfo.Authentication.GetAuthMethodName() |> Expect.equal "" "token"
            }

            testTask "PulsarClient exposes updated service info from provider" {
                let provider = ManualServiceInfoProvider(ServiceInfo("pulsar://localhost:6650"))
                let client =
                    PulsarClient(
                        { PulsarClientConfiguration.Default with
                            ServiceInfoProvider = Some provider })

                let updatedServiceInfo = ServiceInfo("http://localhost:8080")
                provider.Update(updatedServiceInfo)

                client.ServiceInfo.ServiceUrl |> Expect.equal "" "http://localhost:8080"
                client.ServiceInfo.Scheme |> Expect.equal "" "http"
                do! client.CloseAsync()
            }
        ]
