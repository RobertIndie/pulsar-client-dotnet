namespace Pulsar.Client.Internal

open System.Threading.Tasks
open Pulsar.Client.Api
open Pulsar.Client.Common

type internal DynamicLookupService(config: PulsarClientConfiguration,
                                   connectionPool: ConnectionPool,
                                   serviceInfoManager: ServiceInfoManager) =

    let binaryLookupService = BinaryLookupService(config, connectionPool, serviceInfoManager) :> ILookupService
    let httpLookupService = HttpLookupService(config, connectionPool, serviceInfoManager) :> ILookupService

    let getLookupService() =
        let serviceInfo = serviceInfoManager.GetCurrent()
        if serviceInfo.Scheme = ServiceUri.HTTP_SERVICE then
            httpLookupService
        else
            binaryLookupService

    interface ILookupService with

        member _.GetPartitionsForTopic(topicName: TopicName) : Task<TopicName[]> =
            (getLookupService()).GetPartitionsForTopic(topicName)

        member _.GetPartitionedTopicMetadata(topicName: CompleteTopicName) : Task<PartitionedTopicMetadata> =
            (getLookupService()).GetPartitionedTopicMetadata(topicName)

        member _.GetBroker(topicName: CompleteTopicName) : Task<Broker> =
            (getLookupService()).GetBroker(topicName)

        member _.GetTopicsUnderNamespace(ns: NamespaceName, isPersistent: bool) : Task<string[]> =
            (getLookupService()).GetTopicsUnderNamespace(ns, isPersistent)

        member _.GetSchema(topicName: CompleteTopicName, ?schema: SchemaVersion) : Task<TopicSchema option> =
            (getLookupService()).GetSchema(topicName, ?schema = schema)
