namespace Pulsar.Client.Internal

open System.Collections.Generic
open System.Security.Cryptography.X509Certificates
open System.Threading
open Pulsar.Client.Api

type internal ServiceInfoManager(initialServiceInfo: ServiceInfo) =

    let trackedServiceInfos = ResizeArray<ServiceInfo>()
    let mutable current = initialServiceInfo

    do trackedServiceInfos.Add(initialServiceInfo)

    member _.GetCurrent() =
        Volatile.Read(&current)

    member _.Update(serviceInfo: ServiceInfo) =
        trackedServiceInfos.Add(serviceInfo)
        Interlocked.Exchange(&current, serviceInfo) |> ignore

    member _.DisposeTrackedResources() =
        let auths = HashSet<Authentication>(HashIdentity.Reference)
        let certificates = HashSet<X509Certificate2>(HashIdentity.Reference)

        for serviceInfo in trackedServiceInfos do
            if auths.Add(serviceInfo.Authentication) then
                serviceInfo.Authentication.Dispose()

            if not (isNull serviceInfo.TlsTrustCertificate) && certificates.Add(serviceInfo.TlsTrustCertificate) then
                serviceInfo.TlsTrustCertificate.Dispose()

            if not (isNull serviceInfo.TlsCertificate) && certificates.Add(serviceInfo.TlsCertificate) then
                serviceInfo.TlsCertificate.Dispose()
