namespace Pulsar.Client.UnitTests.Api

open System
open Expecto
open Expecto.Flip
open Pulsar.Client.Api
open Pulsar.Client.UnitTests

module ServiceInfoTests =

    [<Tests>]
    let tests =
        testList "ServiceInfoTests" [
            test "ServiceInfo parses url into scheme and addresses" {
                let serviceInfo = ServiceInfo("pulsar://host1:6650,host2:6651/path")

                serviceInfo.Scheme |> Expect.equal "" "pulsar"
                serviceInfo.UseTls |> Expect.equal "" false
                serviceInfo.ServiceAddresses.Length |> Expect.equal "" 2
                serviceInfo.ServiceAddresses[0].Host |> Expect.equal "" "host1"
                serviceInfo.ServiceAddresses[1].Host |> Expect.equal "" "host2"
            }

            test "ServiceInfo accepts explicit tls override" {
                let serviceInfo = ServiceInfo("pulsar://host1:6650", Authentication.AuthenticationDisabled, null, null, true)

                serviceInfo.UseTls |> Expect.equal "" true
            }
        ]

