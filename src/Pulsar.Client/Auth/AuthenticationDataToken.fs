namespace Pulsar.Client.Auth

open System.Collections.Generic
open Pulsar.Client.Api

type internal AuthenticationDataToken (supplier: unit -> string) =
    inherit AuthenticationDataProvider()

    override this.HasDataFromCommand() =
        true

    override this.GetCommandData() =
        supplier()

    override this.HasDataForHttp() =
        true

    override this.GetHttpHeaders() =
        let headers = Dictionary<string, string>()
        headers.Add("X-Pulsar-Auth-Method-Name", "token")
        headers.Add("Authorization", "Bearer " + supplier())
        headers
