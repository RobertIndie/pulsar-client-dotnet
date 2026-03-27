namespace Pulsar.Client.Internal

open System
open Pulsar.Client.Api

type internal DefaultServiceInfoProvider(config: PulsarClientConfiguration) =
    inherit ServiceInfoProvider()

    let getServiceUrl() =
        if String.IsNullOrWhiteSpace config.ServiceUrl then
            invalidArg "config.ServiceUrl" "ServiceUrl needs to be specified when ServiceInfoProvider is not configured."

        config.ServiceUrl

    override _.InitialServiceInfo() =
        ServiceInfo(getServiceUrl(), config.Authentication, config.TlsTrustCertificate, config.TlsCertificate, config.UseTls)

