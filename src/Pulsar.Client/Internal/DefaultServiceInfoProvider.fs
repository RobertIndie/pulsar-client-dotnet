namespace Pulsar.Client.Internal

open Pulsar.Client.Api
open Pulsar.Client.Common

type internal DefaultServiceInfoProvider(config: PulsarClientConfiguration) =
    inherit ServiceInfoProvider()

    let getServiceUrl() =
        ServiceUri.build config.Scheme config.UseTls config.ServiceAddresses

    override _.InitialServiceInfo() =
        ServiceInfo(getServiceUrl(), config.Authentication, config.TlsTrustCertificate, config.TlsCertificate, config.UseTls)

