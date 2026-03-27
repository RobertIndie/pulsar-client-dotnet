namespace Pulsar.Client.Api

open System
open System.Security.Cryptography.X509Certificates
open Pulsar.Client.Common

type ServiceInfo(serviceUrl: string,
                 authentication: Authentication,
                 ?tlsTrustCertificate: X509Certificate2,
                 ?tlsCertificate: X509Certificate2,
                 ?useTls: bool) =

    let parsed =
        match ServiceUri.parse serviceUrl with
        | Ok serviceUri -> serviceUri
        | Error message -> invalidArg "serviceUrl" message

    let authentication =
        authentication
        |> invalidArgIfDefault "authentication can't be null"

    let tlsTrustCertificate = defaultArg tlsTrustCertificate null
    let tlsCertificate = defaultArg tlsCertificate null
    let useTls = defaultArg useTls parsed.UseTls

    member _.ServiceUrl = serviceUrl
    member _.Authentication = authentication
    member _.TlsTrustCertificate = tlsTrustCertificate
    member _.TlsCertificate = tlsCertificate
    member _.UseTls = useTls
    member _.Scheme = parsed.Scheme
    member _.ServiceAddresses = parsed.Addresses

    new(serviceUrl: string) =
        ServiceInfo(serviceUrl, Authentication.AuthenticationDisabled)

    new(serviceUrl: string, authentication: Authentication) =
        ServiceInfo(serviceUrl, authentication, null, null)

