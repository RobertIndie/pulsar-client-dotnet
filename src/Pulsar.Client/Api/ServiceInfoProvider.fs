namespace Pulsar.Client.Api

open System

[<AbstractClass>]
type ServiceInfoProvider() =

    abstract member InitialServiceInfo: unit -> ServiceInfo

    abstract member Initialize: (ServiceInfo -> unit) -> unit
    default _.Initialize _ = ()

    abstract member Dispose: unit -> unit
    default _.Dispose () = ()

    interface IDisposable with
        member this.Dispose() = this.Dispose()

