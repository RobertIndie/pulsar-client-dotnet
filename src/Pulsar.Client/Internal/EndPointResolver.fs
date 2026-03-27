namespace Pulsar.Client.Internal

open System
open System.Net
open System.Threading

type internal EndPointResolver(addressProvider: unit -> Uri list) =
    let mutable currentIndex = -1

    member _.Resolve() =
        let addresses = addressProvider()
        if List.isEmpty addresses then
            invalidArg "addresses" "Addresses list could not be empty."

        let index = Interlocked.Increment(&currentIndex)
        let uri = addresses.[index % addresses.Length]
        DnsEndPoint(uri.Host, uri.Port)
