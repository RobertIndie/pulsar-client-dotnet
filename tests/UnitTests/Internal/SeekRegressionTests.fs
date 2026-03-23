module Pulsar.Client.UnitTests.Internal.SeekRegressionTests

open System
open System.Buffers
open System.Collections.Generic
open System.IO
open System.IO.Pipelines
open System.Net
open System.Diagnostics
open System.Threading
open System.Threading.Tasks
open Expecto
open Expecto.Flip
open FSharp.UMX
open ProtoBuf
open Pulsar.Client.Api
open Pulsar.Client.Common
open Pulsar.Client.Internal
open Pulsar.Client.Schema
open Pulsar.Client.Transaction
open Pulsar.Client.UnitTests
open pulsar.proto

let private serializeSimpleCommand (command: BaseCommand) =
    use temp = new MemoryStream()
    use binaryWriter = new BinaryWriter(temp)

    for _ in 1..4 do
        temp.WriteByte(0uy)

    Serializer.SerializeWithLengthPrefix(temp, command, PrefixStyle.Fixed32BigEndian)
    let frameSize = int temp.Length
    let totalSize = frameSize - 4

    temp.Seek(0L, SeekOrigin.Begin) |> ignore
    binaryWriter.Write(int32ToBigEndian totalSize)
    temp.ToArray()

let private deserializeSimpleCommand (bytes: byte[]) =
    use stream = new MemoryStream(bytes)
    use reader = new BinaryReader(stream)

    let totalSize = reader.ReadInt32() |> int32FromBigEndian
    let commandSize = reader.ReadInt32() |> int32FromBigEndian
    let commandBytes = reader.ReadBytes(commandSize)
    use commandStream = new MemoryStream(commandBytes)
    let command = Serializer.Deserialize<BaseCommand>(commandStream)
    totalSize, commandSize, command

let private readNextBaseCommand (reader: PipeReader) =
    let rec loop () =
        task {
            let! result = reader.ReadAsync()
            let buffer = result.Buffer

            if buffer.Length < 8L then
                reader.AdvanceTo(buffer.Start, buffer.End)
                return! loop ()
            else
                let header = buffer.Slice(0L, 8L).ToArray()
                let totalSize =
                    use stream = new MemoryStream(header)
                    use binaryReader = new BinaryReader(stream)
                    binaryReader.ReadInt32() |> int32FromBigEndian

                let frameLength = int64 (totalSize + 4)
                if buffer.Length < frameLength then
                    reader.AdvanceTo(buffer.Start, buffer.End)
                    return! loop ()
                else
                    let frame = buffer.Slice(0L, frameLength).ToArray()
                    reader.AdvanceTo(buffer.GetPosition(frameLength))
                    let _, _, command = deserializeSimpleCommand frame
                    return command
        }
    loop ()

type private ClientCnxHarness(maxMessageSize: int) =
    let inputPipe = Pipe()
    let outputPipe = Pipe()
    let endpoint = DnsEndPoint("localhost", 6650)
    let broker = { LogicalAddress = LogicalAddress endpoint; PhysicalAddress = PhysicalAddress endpoint }
    let connection =
        {
            Input = inputPipe.Reader
            Output = outputPipe.Writer
            Dispose = fun () -> ()
        }
    let client =
        ClientCnx(
            PulsarClientConfiguration.Default,
            broker,
            connection,
            maxMessageSize,
            TaskCompletionSource<ClientCnx>(TaskCreationOptions.RunContinuationsAsynchronously),
            fun _ -> ()
        )

    member _.Client = client

    member _.ReplySuccessToNextRequest(expectedType: BaseCommand.Type) =
        task {
            let sw = Stopwatch.StartNew()
            let mutable pendingRequestIds = client.PendingRequestIdsForTests()
            while pendingRequestIds.Length = 0 && sw.Elapsed < TimeSpan.FromSeconds(1.0) do
                do! Task.Delay(10)
                pendingRequestIds <- client.PendingRequestIdsForTests()

            Expect.equal "expected a single pending request" 1 pendingRequestIds.Length
            client.CompleteRequestForTests(pendingRequestIds[0], BaseCommand.Type.Success, PulsarResponseType.Empty)
        }

    interface IDisposable with
        member _.Dispose() =
            outputPipe.Writer.Complete()
            outputPipe.Reader.Complete()
            inputPipe.Writer.Complete()
            inputPipe.Reader.Complete()

type private SeekGate() =
    let started = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)
    let finished = TaskCompletionSource<unit>(TaskCreationOptions.RunContinuationsAsynchronously)

    member _.Started = started.Task
    member _.Complete() = finished.TrySetResult() |> ignore
    member _.OnSeek() =
        started.TrySetResult() |> ignore
        finished.Task

type private StubSeekConsumer(topic: string, consumerId: ConsumerId, seekByMessageId: unit -> Task<unit>, seekByResolver: unit -> Task<unit>) =
    interface IConsumer<byte[]> with
        member _.ReceiveAsync() = Task.FromException<Message<byte[]>>(NotSupportedException("not supported") :> exn)
        member _.ReceiveAsync(_: CancellationToken) = Task.FromException<Message<byte[]>>(NotSupportedException("not supported") :> exn)
        member _.BatchReceiveAsync() = Task.FromException<Messages<byte[]>>(NotSupportedException("not supported") :> exn)
        member _.BatchReceiveAsync(_: CancellationToken) = Task.FromException<Messages<byte[]>>(NotSupportedException("not supported") :> exn)
        member _.AcknowledgeAsync(_: MessageId) = Task.FromResult(())
        member _.AcknowledgeAsync(_: MessageId, _: Transaction) = Task.FromResult(())
        member _.AcknowledgeAsync(_: Messages<byte[]>) = Task.FromResult(())
        member _.AcknowledgeAsync(_: MessageId seq) = Task.FromResult(())
        member _.AcknowledgeCumulativeAsync(_: MessageId) = Task.FromResult(())
        member _.AcknowledgeCumulativeAsync(_: MessageId, _: Transaction) = Task.FromResult(())
        member _.RedeliverUnacknowledgedMessagesAsync() = Task.FromResult(())
        member _.UnsubscribeAsync() = Task.FromResult(())
        member _.HasReachedEndOfTopic() = Task.FromResult false
        member _.SeekAsync(_: MessageId) : Task<unit> = seekByMessageId()
        member _.SeekAsync(_: TimeStamp) = Task.FromResult(())
        member _.SeekAsync(_: Func<string, SeekType>) : Task<unit> = seekByResolver()
        member _.GetLastMessageIdAsync() = Task.FromResult MessageId.Earliest
        member _.NegativeAcknowledge(_: MessageId) = Task.FromResult(())
        member _.NegativeAcknowledge(_: Messages<byte[]>) = Task.FromResult(())
        member _.ConsumerId = consumerId
        member _.Topic = topic
        member _.Name = $"stub-{consumerId}"
        member _.GetStats() = Task.FromException<ConsumerStats>(NotSupportedException("not supported") :> exn)
        member _.ReconsumeLaterAsync(_: Message<byte[]>, _: TimeStamp) = Task.FromResult(())
        member _.ReconsumeLaterAsync(_: Messages<byte[]>, _: TimeStamp) = Task.FromResult(())
        member _.ReconsumeLaterCumulativeAsync(_: Message<byte[]>, _: TimeStamp) = Task.FromResult(())
        member _.LastDisconnectedTimestamp() = Task.FromResult %0L
        member _.IsConnected() = Task.FromResult true
    interface IAsyncDisposable with
        member _.DisposeAsync() = ValueTask()

let private unusedLookupService =
    { new ILookupService with
        member _.GetPartitionsForTopic(_) = Task.FromException<TopicName[]>(NotSupportedException("not supported") :> exn)
        member _.GetPartitionedTopicMetadata(_) = Task.FromException<PartitionedTopicMetadata>(NotSupportedException("not supported") :> exn)
        member _.GetBroker(_) = Task.FromException<Broker>(NotSupportedException("not supported") :> exn)
        member _.GetTopicsUnderNamespace(_, _) = Task.FromException<string[]>(NotSupportedException("not supported") :> exn)
        member _.GetSchema(_, ?schema) = Task.FromException<TopicSchema option>(NotSupportedException("not supported") :> exn) }

let private testTopic name = TopicName($"persistent://public/default/{name}")

let private singleTopicConsumerConfig topicName =
    { ConsumerConfiguration.Default with
        Topics = seq { yield topicName }
        ConsumerName = "seek-regression-consumer"
        SubscriptionName = %"seek-regression-subscription"
        SubscriptionType = SubscriptionType.Exclusive
        SubscriptionMode = SubscriptionMode.NonDurable
        SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
        AutoUpdatePartitions = false
        RetryEnable = false }

let private multiTopicConsumerConfig =
    { ConsumerConfiguration.Default with
        Topics = Seq.empty
        ConsumerName = "multitopic-seek-regression-consumer"
        SubscriptionName = %"multitopic-seek-regression-subscription"
        SubscriptionType = SubscriptionType.Exclusive
        SubscriptionMode = SubscriptionMode.NonDurable
        SubscriptionInitialPosition = SubscriptionInitialPosition.Latest
        AutoUpdatePartitions = false
        RetryEnable = false }

let private consumerCleanup (_: ConsumerImpl<byte[]>) = ()

let private multiTopicCleanup (_: MultiTopicsConsumerImpl<byte[]>) = ()

let private makeCommandMessage (consumerId: ConsumerId) (messageId: MessageId) payload =
    let cmd =
        CommandMessage(
            ConsumerId = %consumerId,
            MessageId = MessageIdData(ledgerId = uint64 %messageId.LedgerId, entryId = uint64 %messageId.EntryId)
        )
    let metadata =
        MessageMetadata(
            ProducerName = "seek-regression-producer",
            PublishTime = uint64 (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()),
            SequenceId = 1UL
        )
    XCommandMessage(cmd, metadata, new MemoryStream(payload: byte[]), true)

let private makeRawMessage (messageId: MessageId) (payload: byte[]) =
    {
        MessageId = messageId
        Metadata =
            {
                NumMessages = 1
                NumChunks = 0
                TotalChunkMsgSize = 0
                HasNumMessagesInBatch = false
                CompressionType = Pulsar.Client.Common.CompressionType.None
                UncompressedMessageSize = payload.Length
                SchemaVersion = None
                SequenceId = %1L
                ChunkId = %0
                PublishTime = %DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                Uuid = %""
                EncryptionKeys = [||]
                EncryptionParam = [||]
                EncryptionAlgo = ""
                EventTime = Nullable()
                OrderingKey = [||]
                ReplicatedFrom = ""
                ProducerName = "seek-regression-producer"
                NullValue = false
            }
        RedeliveryCount = 0
        Payload = new MemoryStream(payload: byte[])
        MessageKey = ""
        IsKeyBase64Encoded = false
        CheckSumValid = true
        Properties = EmptyProps
        AckSet = EmptyAckSet
    }

let private applyPostedOperations (consumers: Dictionary<ConsumerId, ConsumerOperations>) (posted: ResizeArray<CnxOperation>) =
    for operation in posted do
        match operation with
        | RemoveConsumer consumerId ->
            consumers.Remove consumerId |> ignore
        | _ ->
            ()
    posted.Clear()

let private awaitWithTimeout timeoutMessage (work: Task<'T>) =
    task {
        let! completed = Task.WhenAny(work, Task.Delay(1000))
        if not (obj.ReferenceEquals(completed, work)) then
            failtestf "%s" timeoutMessage
        return! work
    }

let private awaitUnitWithTimeout timeoutMessage (work: Task) =
    task {
        let! completed = Task.WhenAny(work, Task.Delay(1000))
        if not (obj.ReferenceEquals(completed, work)) then
            failtestf "%s" timeoutMessage
        do! work
    }

let private assertNoMessageDeliveredAfterSeek description (consumer: IConsumer<byte[]>) =
    let cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100.0))
    try
        let received = consumer.ReceiveAsync(cts.Token).GetAwaiter().GetResult()
        failtestf "%s. Received stale message %A" description received.MessageId
    with
    | :? TaskCanceledException ->
        ()
    cts.Dispose()

let private makeMessage messageId payload =
    Message(
        messageId,
        payload,
        %"",
        false,
        EmptyProps,
        None,
        [||],
        %0L,
        [||],
        %0L,
        Nullable(),
        0,
        "",
        "",
        fun () -> payload
    )

[<Tests>]
let tests =
    testList "Seek regression tests" [
        testTask "Single topic seek should not deliver stale message through ReceiveAsync" {
            let activeCnx = new ClientCnxHarness(Commands.DEFAULT_MAX_MESSAGE_SIZE)
            let dispatchCnx = new ClientCnxHarness(Commands.DEFAULT_MAX_MESSAGE_SIZE)
            let topic = testTopic "seek-single-topic"
            let consumer: ConsumerImpl<byte[]> =
                ConsumerImpl<byte[]>(
                    (singleTopicConsumerConfig topic),
                    PulsarClientConfiguration.Default,
                    topic,
                    Unchecked.defaultof<ConnectionPool>,
                    -1,
                    false,
                    None,
                    TimeSpan.Zero,
                    unusedLookupService,
                    false,
                    Schema.BYTES(),
                    None,
                    ConsumerInterceptors.Empty,
                    consumerCleanup
                )

            consumer.SetReady(activeCnx.Client)

            let targetMessageId = { LedgerId = %10L; EntryId = %10L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let staleMessageId = { LedgerId = %9L; EntryId = %9L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let targetBytes = targetMessageId.ToByteArray()
            let targetMessageIdFromBytes = MessageId.FromByteArray targetBytes
            let replyTask = activeCnx.ReplySuccessToNextRequest(BaseCommand.Type.Seek)

            do! awaitUnitWithTimeout "single-topic seek timed out" ((consumer :> IConsumer<byte[]>).SeekAsync(Func<string, SeekType>(fun _ -> SeekType.MessageId(targetMessageIdFromBytes))))
            do! awaitUnitWithTimeout "seek reply completion timed out" replyTask

            let consumers = Dictionary<ConsumerId, ConsumerOperations>()
            consumers[consumer.ConsumerId] <- consumer.ConsumerOperations
            let postedOperations = ResizeArray<CnxOperation>()

            dispatchCnx.Client.DispatchConsumerCommandForTests(
                consumers,
                postedOperations.Add,
                XCommandCloseConsumer(CommandCloseConsumer(ConsumerId = %consumer.ConsumerId)))

            let reconnectReplyTask = activeCnx.ReplySuccessToNextRequest(BaseCommand.Type.Success)
            post consumer.Mb ConsumerMessage.ConnectionOpened
            do! awaitUnitWithTimeout "single-topic reconnect subscribe timed out" reconnectReplyTask

            consumer.ConsumerOperations.MessageReceived(struct(makeRawMessage staleMessageId [| 7uy; 8uy; 9uy |], dispatchCnx.Client))

            assertNoMessageDeliveredAfterSeek "single-topic seek delivered a stale message" (consumer :> IConsumer<byte[]>)
            (activeCnx :> IDisposable).Dispose()
            (dispatchCnx :> IDisposable).Dispose()
        }

        testTask "Single topic control ignores stale message when reconnect has not happened yet" {
            let activeCnx = new ClientCnxHarness(Commands.DEFAULT_MAX_MESSAGE_SIZE)
            let dispatchCnx = new ClientCnxHarness(Commands.DEFAULT_MAX_MESSAGE_SIZE)
            let topic = testTopic "seek-single-topic-control"
            let consumer: ConsumerImpl<byte[]> =
                ConsumerImpl<byte[]>(
                    (singleTopicConsumerConfig topic),
                    PulsarClientConfiguration.Default,
                    topic,
                    Unchecked.defaultof<ConnectionPool>,
                    -1,
                    false,
                    None,
                    TimeSpan.Zero,
                    unusedLookupService,
                    false,
                    Schema.BYTES(),
                    None,
                    ConsumerInterceptors.Empty,
                    consumerCleanup
                )

            consumer.SetReady(activeCnx.Client)

            let targetMessageId = { LedgerId = %10L; EntryId = %10L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            let targetBytes = targetMessageId.ToByteArray()
            let targetMessageIdFromBytes = MessageId.FromByteArray targetBytes
            let replyTask = activeCnx.ReplySuccessToNextRequest(BaseCommand.Type.Seek)

            do! awaitUnitWithTimeout "single-topic control seek timed out" ((consumer :> IConsumer<byte[]>).SeekAsync(Func<string, SeekType>(fun _ -> SeekType.MessageId(targetMessageIdFromBytes))))
            do! awaitUnitWithTimeout "single-topic control seek reply timed out" replyTask

            let consumers = Dictionary<ConsumerId, ConsumerOperations>()
            consumers[consumer.ConsumerId] <- consumer.ConsumerOperations
            let postedOperations = ResizeArray<CnxOperation>()
            let staleMessageId = { LedgerId = %9L; EntryId = %9L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }

            dispatchCnx.Client.DispatchConsumerCommandForTests(
                consumers,
                postedOperations.Add,
                XCommandCloseConsumer(CommandCloseConsumer(ConsumerId = %consumer.ConsumerId)))
            consumer.ConsumerOperations.MessageReceived(struct(makeRawMessage staleMessageId [| 1uy |], dispatchCnx.Client))
            let cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100.0))
            Expect.throwsT2<TaskCanceledException>(fun () ->
                (consumer :> IConsumer<byte[]>).ReceiveAsync(cts.Token).GetAwaiter().GetResult() |> ignore)
            |> ignore
            cts.Dispose()
            (activeCnx :> IDisposable).Dispose()
            (dispatchCnx :> IDisposable).Dispose()
        }

        testTask "Multi topic seek should not deliver stale message through ReceiveAsync" {
            let topic1 = testTopic "seek-multi-topic-1"
            let topic2 = testTopic "seek-multi-topic-2"
            let gate1 = SeekGate()
            let gate2 = SeekGate()
            let child1 = StubSeekConsumer((string topic1), %1UL, gate1.OnSeek, fun () -> Task.FromResult(()))
            let child2 = StubSeekConsumer((string topic2), %2UL, gate2.OnSeek, fun () -> Task.FromResult(()))
            let consumer: MultiTopicsConsumerImpl<byte[]> =
                MultiTopicsConsumerImpl<byte[]>(
                    multiTopicConsumerConfig,
                    PulsarClientConfiguration.Default,
                    Unchecked.defaultof<ConnectionPool>,
                    MultiConsumerType.MultiTopic [||],
                    None,
                    TimeSpan.Zero,
                    unusedLookupService,
                    ConsumerInterceptors.Empty,
                    multiTopicCleanup
                )

            consumer.SetCurrentStreamForTests(TaskSeq(Seq.empty))
            consumer.AddConsumerForTests(topic1.CompleteTopicName, child1, fun () -> Task.FromResult(Ok (makeMessage MessageId.Earliest [||])))
            consumer.AddConsumerForTests(topic2.CompleteTopicName, child2, fun () -> Task.FromResult(Ok (makeMessage MessageId.Earliest [||])))

            let seekTask = (consumer :> IConsumer<byte[]>).SeekAsync(MessageId.Earliest)
            do! awaitUnitWithTimeout "multi-topic message-id seek did not start both child seeks" (Task.WhenAll(gate1.Started, gate2.Started))

            let staleMessageId = { LedgerId = %3L; EntryId = %3L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            do! awaitUnitWithTimeout "multi-topic stale message injection timed out" (consumer.InjectPolledMessageForTests(Ok (makeMessage staleMessageId [| 3uy |])))

            gate1.Complete()
            gate2.Complete()
            do! awaitUnitWithTimeout "multi-topic message-id seek timed out" seekTask

            assertNoMessageDeliveredAfterSeek "multi-topic message-id seek delivered a stale message" (consumer :> IConsumer<byte[]>)
        }

        testTask "Multi topic resolver seek should not deliver stale message through ReceiveAsync" {
            let topic1 = testTopic "seek-multi-topic-resolver-1"
            let topic2 = testTopic "seek-multi-topic-resolver-2"
            let gate1 = SeekGate()
            let gate2 = SeekGate()
            let child1 = StubSeekConsumer((string topic1), %3UL, (fun () -> Task.FromResult(())), gate1.OnSeek)
            let child2 = StubSeekConsumer((string topic2), %4UL, (fun () -> Task.FromResult(())), gate2.OnSeek)
            let consumer: MultiTopicsConsumerImpl<byte[]> =
                MultiTopicsConsumerImpl<byte[]>(
                    multiTopicConsumerConfig,
                    PulsarClientConfiguration.Default,
                    Unchecked.defaultof<ConnectionPool>,
                    MultiConsumerType.MultiTopic [||],
                    None,
                    TimeSpan.Zero,
                    unusedLookupService,
                    ConsumerInterceptors.Empty,
                    multiTopicCleanup
                )

            consumer.SetCurrentStreamForTests(TaskSeq(Seq.empty))
            consumer.AddConsumerForTests(topic1.CompleteTopicName, child1, fun () -> Task.FromResult(Ok (makeMessage MessageId.Earliest [||])))
            consumer.AddConsumerForTests(topic2.CompleteTopicName, child2, fun () -> Task.FromResult(Ok (makeMessage MessageId.Earliest [||])))

            let seekTask =
                (consumer :> IConsumer<byte[]>).SeekAsync(Func<string, SeekType>(fun _ -> SeekType.MessageId(MessageId.Earliest)))
            do! awaitUnitWithTimeout "multi-topic resolver seek did not start both child seeks" (Task.WhenAll(gate1.Started, gate2.Started))

            let staleMessageId = { LedgerId = %4L; EntryId = %4L; Partition = -1; Type = Single; TopicName = %""; ChunkMessageIds = None }
            do! awaitUnitWithTimeout "multi-topic resolver stale message injection timed out" (consumer.InjectPolledMessageForTests(Ok (makeMessage staleMessageId [| 4uy |])))

            gate1.Complete()
            gate2.Complete()
            do! awaitUnitWithTimeout "multi-topic resolver seek timed out" seekTask

            assertNoMessageDeliveredAfterSeek "multi-topic resolver seek delivered a stale message" (consumer :> IConsumer<byte[]>)
        }
    ]
