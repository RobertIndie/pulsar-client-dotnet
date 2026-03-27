namespace Pulsar.Client.Api

open System
open Pulsar.Client.Common


type PulsarClientBuilder private (config: PulsarClientConfiguration, serviceUrlConfigured: bool) =

    [<Literal>]
    let MIN_STATS_INTERVAL_SECONDS = 1

    let verify(config : PulsarClientConfiguration) =
        let checkValue check config =
            check config |> ignore
            config

        config
        |> checkValue
            (fun c ->
                match c.ServiceInfoProvider, c.ServiceAddresses |> List.isEmpty, serviceUrlConfigured with
                | Some _, _, true -> invalidArg "config" "ServiceUrl and ServiceInfoProvider cannot be configured together."
                | None, true, _ -> invalidArg "config" "ServiceUrl or ServiceInfoProvider needs to be specified on the PulsarClientBuilder object."
                | _ -> ())

    new() = PulsarClientBuilder(PulsarClientConfiguration.Default, false)

    member this.ServiceUrl (url: string) =
        match url |> ServiceUri.parse with
        | (Result.Ok serviceUri) ->
            PulsarClientBuilder({ config with ServiceAddresses = serviceUri.Addresses; UseTls = serviceUri.UseTls ; Scheme = serviceUri.Scheme }, true)
        | (Result.Error message) -> invalidArg null message

    member this.ServiceInfoProvider (serviceInfoProvider: ServiceInfoProvider) =
        PulsarClientBuilder(
            { config with
                ServiceInfoProvider = Some (serviceInfoProvider |> invalidArgIfDefault "ServiceInfoProvider can't be null") },
            serviceUrlConfigured)

    member this.OperationTimeout operationTimeout =
        PulsarClientBuilder({ config with OperationTimeout = operationTimeout }, serviceUrlConfigured)

    member this.MaxNumberOfRejectedRequestPerConnection num =
        PulsarClientBuilder({ config with MaxNumberOfRejectedRequestPerConnection = num |> invalidArgIfLessThanZero "MaxNumberOfRejectedRequestPerConnection can't be negative" }, serviceUrlConfigured)

    member this.EnableTls useTls =
        PulsarClientBuilder({ config with UseTls = useTls }, serviceUrlConfigured)

    member this.EnableTlsHostnameVerification enableTlsHostnameVerification =
        PulsarClientBuilder({ config with TlsHostnameVerificationEnable = enableTlsHostnameVerification }, serviceUrlConfigured)

    member this.AllowTlsInsecureConnection allowTlsInsecureConnection =
        PulsarClientBuilder({ config with TlsAllowInsecureConnection = allowTlsInsecureConnection }, serviceUrlConfigured)

    member this.TlsTrustCertificate tlsTrustCertificate =
        PulsarClientBuilder({ config with TlsTrustCertificate = tlsTrustCertificate }, serviceUrlConfigured)
            
    member this.TlsCertificate tlsCertificate =
        PulsarClientBuilder({ config with TlsCertificate = tlsCertificate }, serviceUrlConfigured)

    member this.Authentication authentication =
        PulsarClientBuilder({ config with Authentication = authentication |> invalidArgIfDefault "Authentication can't be null" }, serviceUrlConfigured)

    member this.TlsProtocols protocol =
        PulsarClientBuilder({ config with TlsProtocols = protocol }, serviceUrlConfigured)

    member this.StatsInterval interval =
        PulsarClientBuilder({ config with
                                StatsInterval = interval |> invalidArgIf (fun arg ->
                                arg <> TimeSpan.Zero && arg < TimeSpan.FromSeconds(float MIN_STATS_INTERVAL_SECONDS)) (sprintf "Stats interval should be greater than %i s" MIN_STATS_INTERVAL_SECONDS) }, serviceUrlConfigured)

    member this.ListenerName name =
        PulsarClientBuilder({ config with
                                ListenerName =
                                    name
                                    |> invalidArgIfBlankString "Param listenerName must not be blank."
                                    |> _.Trim() }, serviceUrlConfigured)

    member this.MaxLookupRedirects maxLookupRedirects =
        PulsarClientBuilder({ config with MaxLookupRedirects = maxLookupRedirects }, serviceUrlConfigured)

    member this.EnableTransaction enableTransaction =
        PulsarClientBuilder({ config with EnableTransaction = enableTransaction }, serviceUrlConfigured)

    member this.KeepAliveInterval keepAliveInterval =
        PulsarClientBuilder({ config with KeepAliveInterval = keepAliveInterval }, serviceUrlConfigured)

    member this.BuildAsync() =
        let client =
            config
            |> verify
            |> PulsarClient
        backgroundTask {
            do! client.Init()
            return client
        }


    member this.Configuration =
        config
