module Pulsar.Client.IntegrationTests.Seek

open System
open System.Threading
open System.Diagnostics
open System.Collections.Generic

open Expecto
open Expecto.Flip

open System.Text
open System.Threading.Tasks
open FSharp.UMX
open Pulsar.Client.Api
open Pulsar.Client.Common
open Serilog
open Pulsar.Client.IntegrationTests
open Pulsar.Client.IntegrationTests.Common

let private createNonDurableLatestConsumer (client: PulsarClient) topicName subscriptionName receiverQueueSize =
    client.NewConsumer()
        .Topic(topicName)
        .ConsumerName(Guid.NewGuid().ToString("N"))
        .SubscriptionType(SubscriptionType.Exclusive)
        .SubscriptionInitialPosition(SubscriptionInitialPosition.Latest)
        .SubscriptionMode(SubscriptionMode.NonDurable)
        .SubscriptionName(subscriptionName)
        .ReceiverQueueSize(receiverQueueSize)
        .EnableRetry(false)
        .SubscribeAsync()

let private createNonDurableLatestConsumerWithAckGroup (client: PulsarClient) topicName subscriptionName receiverQueueSize ackGroupTime =
    client.NewConsumer()
        .Topic(topicName)
        .ConsumerName(Guid.NewGuid().ToString("N"))
        .SubscriptionType(SubscriptionType.Exclusive)
        .SubscriptionInitialPosition(SubscriptionInitialPosition.Latest)
        .SubscriptionMode(SubscriptionMode.NonDurable)
        .SubscriptionName(subscriptionName)
        .ReceiverQueueSize(receiverQueueSize)
        .AcknowledgementsGroupTime(ackGroupTime)
        .EnableRetry(false)
        .SubscribeAsync()

let private encodeSequence sequence =
    Encoding.UTF8.GetBytes(string sequence)

let private decodeSequence (message: Message<byte[]>) =
    message.GetValue()
    |> Encoding.UTF8.GetString
    |> int

let private produceSequencedMessages (producer: IProducer<byte[]>) startSequence count : Task<MessageId[]> =
    task {
        let messageIds = Array.zeroCreate<MessageId> count
        for offset in 0..(count-1) do
            let sequence = startSequence + offset
            let! messageId = producer.SendAsync(encodeSequence sequence)
            messageIds.[offset] <- messageId
        return messageIds
    }

let private receiveAndAcknowledgeSequence (consumer: IConsumer<byte[]>) expectedSequences (timeout: TimeSpan) context =
    task {
        for expectedSequence in expectedSequences do
            use cts = new CancellationTokenSource(timeout)
            let! message = consumer.ReceiveAsync(cts.Token)
            let actualSequence = decodeSequence message
            Expect.equal
                (sprintf "%s: unexpected sequence while consuming after seek" context)
                expectedSequence
                actualSequence
            do! consumer.AcknowledgeAsync(message.MessageId)
    }

let private receiveWithTimeout (consumer: IConsumer<byte[]>) (timeout: TimeSpan) =
    task {
        use cts = new CancellationTokenSource(timeout)
        return! consumer.ReceiveAsync(cts.Token)
    }

let private unloadTopic topicName =
    task {
        let url = $"{pulsarHttpAddress}/admin/v2/persistent/{topicName}/unload"
        use! response = commonHttpClient.PutAsync(url, null)
        response.EnsureSuccessStatusCode() |> ignore
    }

let private seekUsingSerializedMessageId (consumer: IConsumer<byte[]>) (messageId: MessageId) =
    let serializedMessageId = messageId.ToByteArray()
    let deserializedMessageId = MessageId.FromByteArray(serializedMessageId)
    consumer.SeekAsync(Func<string, SeekType>(fun _ -> SeekType.MessageId deserializedMessageId))

[<Tests>]
let tests =

    let testRandomSeek (enableBatching: bool) =
        task {
            Log.Debug("Started Seek randomly works, batching {0}", enableBatching)
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekRandomProducer"
            let consumerName = "seekRandomConsumer"
            let numberOfMessages = 10
            let numberOfRandomSeeks = 10
            let producedMessageIds = Array.zeroCreate<MessageId> numberOfMessages;

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(enableBatching)
                    .CreateAsync()

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync()
        
            for i in 0..(numberOfMessages-1) do
                let! messageId = producer.SendAsync([| byte i |])
                producedMessageIds.[i] <- messageId
                
            let rand = Random()
            for _ = 1 to numberOfRandomSeeks do
                let index = rand.Next(0, numberOfMessages - 1)
                let messageId = producedMessageIds.[index]
                Log.Debug("Resetting to index {0}, msgId {1}", index, messageId)
                do! consumer.SeekAsync(messageId)
                let! message = consumer.ReceiveAsync()
                Expect.equal "" (byte (index+1)) message.Data.[0]
                
   
            Log.Debug("Finished Seek randomly works, batching {0}", enableBatching)
        }
    
    testList "Seek" [
        
        testTask "Consumer seek earliest redelivers all messages" {

            Log.Debug("Started Consumer seek earliest redelivers all messages")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 100

            let! producer =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync() 

            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 

            let producerTask =
                Task.Run(fun () ->
                    task {
                        do! produceMessages producer numberOfMessages producerName
                    }:> Task)

            let consumerTask =
                Task.Run(fun () ->
                    task {
                        do! consumeMessages consumer numberOfMessages consumerName
                    }:> Task)

            do! Task.WhenAll(producerTask, consumerTask) 
            do! consumer.SeekAsync(MessageId.Earliest) 
            do! consumeMessages consumer numberOfMessages consumerName 

            Log.Debug("Finished Consumer seek earliest redelivers all messages")
        }
        
        testTask "Consumer seek can be done to serialized message" {

            Log.Debug("Started Consumer seek can be done to serialized message")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"

            let! (producer : IProducer<string>) =
                client.NewProducer(Schema.STRING())
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .CreateAsync() 

            let! (consumer : IConsumer<string>) =
                client.NewConsumer(Schema.STRING())
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
            
            let! (msgId1 : MessageId) = producer.SendAsync("Hello1") 
            let! msgId2 = producer.SendAsync("Hello2") 
            let serializedMsgId = msgId1.ToByteArray()
            let deserializedMsgId = MessageId.FromByteArray(serializedMsgId)

            do! consumer.SeekAsync(deserializedMsgId) 
            let! (msg : Message<string>) = consumer.ReceiveAsync() 
            
            Expect.equal "" "Hello2" <| msg.GetValue()
            Log.Debug("Finished Consumer seek can be done to serialized message")
        }
        
        testTask "Seek in the middle of the batch works properly" {

            Log.Debug("Started Seek in the middle of the batch works properly")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .StartMessageIdInclusive()
                    .SubscribeAsync() 

            do! fastProduceMessages producer numberOfMessages producerName 
            let! (message1 : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message2 : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message3 : Message<byte[]>) = consumer.ReceiveAsync() 
            do!
                [|
                  consumer.AcknowledgeAsync(message1.MessageId)
                  consumer.AcknowledgeAsync(message2.MessageId)
                  consumer.AcknowledgeAsync(message3.MessageId)
                |]
                |> Task.WhenAll 
            do! Task.Delay 110
            do! consumer.SeekAsync(message2.MessageId) 
            let! (message2x : Message<byte[]>) = consumer.ReceiveAsync() 
            let! (message3x : Message<byte[]>) = consumer.ReceiveAsync() 
            do!
                [|
                  consumer.AcknowledgeAsync(message2x.MessageId)
                  consumer.AcknowledgeAsync(message3x.MessageId)
                |] |> Task.WhenAll   
             
            Expect.equal "" message2.MessageId message2x.MessageId
            Expect.equal "" message3.MessageId message3x.MessageId
            
            Log.Debug("Finished Seek in the middle of the batch works properly")
        }
        
        testTask "Seek in the middle of the batch works properly 2" {

            Log.Debug("Started Seek in the middle of the batch works properly 2")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let producerName = "seekProducer"
            let consumerName = "seekConsumer"
            let numberOfMessages = 3

            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(true)
                    .BatchingMaxMessages(numberOfMessages)
                    .BatchingMaxPublishDelay(TimeSpan.FromSeconds(50.0))
                    .CreateAsync() 

            let! (consumer : IConsumer<byte[]>) =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName("test-subscription")
                    .SubscribeAsync() 
        
            let tasks =
                [|
                    producer.SendAsync(Encoding.UTF8.GetBytes("1"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("2"))
                    producer.SendAsync(Encoding.UTF8.GetBytes("3"))
                |]
                
            let! (msgIds : MessageId[]) = tasks |> Task.WhenAll 
            
            do! consumer.SeekAsync(msgIds.[1]) 
            let! (msg : Message<byte[]>) = consumer.ReceiveAsync() 
            
            Expect.equal "" "3" (msg.GetValue() |> Encoding.UTF8.GetString )
   
            Log.Debug("Finished Seek in the middle of the batch works properly 2")
        }
        
        testTask "Seek randomly works with batching " {
            do! testRandomSeek true 
        }
        
        testTask "Seek randomly works without batching " {
            do! testRandomSeek true 
        }

        testTask "Non-durable consumer seek stays stable after topic unload" {

            Log.Debug("Started Non-durable consumer seek stays stable after topic unload")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "seek-nondurable-unload-" + Guid.NewGuid().ToString("N")
            let baseMessageCount = 40
            let tailMessageCount = 5
            let seekIndex = 14

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("seek-unload-producer")
                    .EnableBatching(false)
                    .CreateAsync()

            let! (baseMessageIds: MessageId[]) = produceSequencedMessages producer 0 baseMessageCount

            let! (consumer: IConsumer<byte[]>) =
                createNonDurableLatestConsumer client topicName subscriptionName 20

            do! seekUsingSerializedMessageId consumer baseMessageIds.[seekIndex]
            do! Task.Delay(500)
            do! unloadTopic topicName
            do! Task.Delay(1000)

            do!
                receiveAndAcknowledgeSequence
                    consumer
                    [seekIndex + 1 .. baseMessageCount - 1]
                    (TimeSpan.FromSeconds(20.0))
                    "after unloading the topic"

            let! _ = produceSequencedMessages producer baseMessageCount tailMessageCount

            do!
                receiveAndAcknowledgeSequence
                    consumer
                    [baseMessageCount .. baseMessageCount + tailMessageCount - 1]
                    (TimeSpan.FromSeconds(10.0))
                    "after publishing new messages post-seek"

            do! consumer.DisposeAsync().AsTask()
            do! producer.DisposeAsync().AsTask()
            Log.Debug("Finished Non-durable consumer seek stays stable after topic unload")
        }

        testTask "Non-durable consumer seek stays stable after repeated recreate and close" {

            Log.Debug("Started Non-durable consumer seek stays stable after repeated recreate and close")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "seek-nondurable-recreate-" + Guid.NewGuid().ToString("N")
            let baseMessageCount = 35
            let tailMessageCount = 5
            let seekIndex = 11
            let warmupCycles = 12

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("seek-recreate-producer")
                    .EnableBatching(false)
                    .CreateAsync()

            let! (baseMessageIds: MessageId[]) = produceSequencedMessages producer 0 baseMessageCount

            for cycle in 1..warmupCycles do
                let! (consumer: IConsumer<byte[]>) =
                    createNonDurableLatestConsumer client topicName subscriptionName 10

                do! seekUsingSerializedMessageId consumer baseMessageIds.[seekIndex]
                do! Task.Delay(200)
                do!
                    receiveAndAcknowledgeSequence
                        consumer
                        [seekIndex + 1; seekIndex + 2]
                        (TimeSpan.FromSeconds(10.0))
                        (sprintf "during warmup cycle %i" cycle)
                do! consumer.DisposeAsync().AsTask()

            let! (consumer: IConsumer<byte[]>) =
                createNonDurableLatestConsumer client topicName subscriptionName 10

            do! seekUsingSerializedMessageId consumer baseMessageIds.[seekIndex]
            do! Task.Delay(200)
            do!
                receiveAndAcknowledgeSequence
                    consumer
                    [seekIndex + 1 .. baseMessageCount - 1]
                    (TimeSpan.FromSeconds(10.0))
                    "after recreating the subscription repeatedly"

            let! _ = produceSequencedMessages producer baseMessageCount tailMessageCount

            do!
                receiveAndAcknowledgeSequence
                    consumer
                    [baseMessageCount .. baseMessageCount + tailMessageCount - 1]
                    (TimeSpan.FromSeconds(10.0))
                    "after recreating the subscription and publishing new messages"

            do! consumer.DisposeAsync().AsTask()
            do! producer.DisposeAsync().AsTask()
            Log.Debug("Finished Non-durable consumer seek stays stable after repeated recreate and close")
        }

        testTask "Non-durable consumer seek does not leak stale messages across seek reconnects" {

            Log.Debug("Started Non-durable consumer seek does not leak stale messages across seek reconnects")
            let client = getClient()
            let topicName = "public/default/topic-" + Guid.NewGuid().ToString("N")
            let subscriptionName = "seek-nondurable-stale-" + Guid.NewGuid().ToString("N")
            let receiverQueueSize = 500
            let ackGroupTime = TimeSpan.FromSeconds(5.0)
            let totalMessages = 2500
            let warmupMessageCount = 120
            let seekCycles = 25
            let messagesToVerifyAfterSeek = 8

            let! (producer: IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName("seek-stale-producer")
                    .EnableBatching(false)
                    .CreateAsync()

            let produceTask =
                Task.Run(fun () ->
                    task {
                        for sequence in 0..(totalMessages - 1) do
                            let! _ = producer.SendAsync(encodeSequence sequence)
                            ()
                    }:> Task)

            let! (initialConsumer: IConsumer<byte[]>) =
                createNonDurableLatestConsumerWithAckGroup client topicName subscriptionName receiverQueueSize ackGroupTime
            let mutable consumer = initialConsumer

            let seen = ResizeArray<int * MessageId>()
            while seen.Count < warmupMessageCount do
                let! message = receiveWithTimeout consumer (TimeSpan.FromSeconds(20.0))
                let sequence = decodeSequence message
                seen.Add(sequence, message.MessageId)
                do! consumer.AcknowledgeAsync(message.MessageId)

            for cycle in 1..seekCycles do
                let targetIndex = seen.Count - 40
                let targetSequence, targetMessageId = seen.[targetIndex]

                do! seekUsingSerializedMessageId consumer targetMessageId

                for expectedSequence in [targetSequence + 1 .. targetSequence + messagesToVerifyAfterSeek] do
                    let! message = receiveWithTimeout consumer (TimeSpan.FromSeconds(20.0))
                    let actualSequence = decodeSequence message
                    Expect.equal
                        (sprintf "stale/leaked message detected after seek cycle %i" cycle)
                        expectedSequence
                        actualSequence
                    seen.Add(actualSequence, message.MessageId)
                    do! consumer.AcknowledgeAsync(message.MessageId)

                if cycle % 5 = 0 then
                    do! consumer.DisposeAsync().AsTask()
                    let! (recreatedConsumer: IConsumer<byte[]>) =
                        createNonDurableLatestConsumerWithAckGroup client topicName subscriptionName receiverQueueSize ackGroupTime
                    consumer <- recreatedConsumer
                    for _ in 1..2 do
                        let! message = receiveWithTimeout consumer (TimeSpan.FromSeconds(20.0))
                        seen.Add(decodeSequence message, message.MessageId)
                        do! consumer.AcknowledgeAsync(message.MessageId)
                    ()

            do! produceTask
            do! consumer.DisposeAsync().AsTask()
            do! producer.DisposeAsync().AsTask()
            Log.Debug("Finished Non-durable consumer seek does not leak stale messages across seek reconnects")
        }
        
        
        testTask "Seek won't get stuck at the receive in MultiTopicsConsumer" {
            Log.Debug("Started Seek won't get stuck at the receive in MultiTopicsConsumer")
            let client = getClient()
            let topicName = "persistent://public/default/multi-topic-seek"
            let producerName = "seekStuckProducer"
            let consumerName = "seekStuckConsumer"
            let numberOfMessages = 30
            let subscriptionName = "test-seek-stuck-" + Guid.NewGuid().ToString("N")
            
            let seekWithRetry (consumer: IConsumer<byte[]>) (targetTimestamp: TimeStamp) (maxRetries: int) =
                task {
                    let mutable retryCount = 0
                    let mutable success = false
                    while retryCount < maxRetries && not success do
                        try
                            do! consumer.SeekAsync(targetTimestamp)
                            success <- true
                        with Flatten ex ->
                            match ex with
                            | :? NotConnectedException as notConnectedEx ->
                                retryCount <- retryCount + 1
                                if retryCount >= maxRetries then
                                    Log.Error("SeekAsync failed after {0} retries: {1}", maxRetries, notConnectedEx.Message)
                                    raise notConnectedEx
                                else
                                    Log.Debug("SeekAsync failed (attempt {0}/{1}): {2}. Retrying in 1 second...", retryCount, maxRetries, notConnectedEx.Message)
                                    do! Task.Delay(1000)
                            | _ ->
                                raise ex
                }
            
            let! consumer =
                client.NewConsumer()
                    .Topic(topicName)
                    .ConsumerName(consumerName)
                    .SubscriptionName(subscriptionName)
                    .ReceiverQueueSize(10)
                    .SubscribeAsync()
            let! (producer : IProducer<byte[]>) =
                client.NewProducer()
                    .Topic(topicName)
                    .ProducerName(producerName)
                    .EnableBatching(false)
                    .CreateAsync()
            
            let expectedMessages = HashSet<string>()
            for i in 1..numberOfMessages do
                let messageContent = sprintf "Message-%i-%s" i (Guid.NewGuid().ToString("N"))
                expectedMessages.Add(messageContent) |> ignore
                let messageBytes = Encoding.UTF8.GetBytes(messageContent)
                let! (_ : MessageId) = producer.SendAsync(messageBytes)
                ()
            do! Task.Delay(1000)
            
            let targetTimestamp = %(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1000L * 3600L * 24L)
            Log.Debug("Seeking to timestamp: {0}", targetTimestamp)
            do! seekWithRetry consumer targetTimestamp 10
            
            let receivedMessages = HashSet<string>()
            let cts = new CancellationTokenSource(TimeSpan.FromSeconds(30.0))
            
            try
                for _ in 1..numberOfMessages do
                    let! (message : Message<byte[]>) = consumer.ReceiveAsync(cts.Token)
                    let received = Encoding.UTF8.GetString(message.Data)
                    Log.Debug("{0} received {1}", consumerName, received)
                    receivedMessages.Add(received) |> ignore
                    do! consumer.AcknowledgeAsync(message.MessageId)
                
                Expect.equal "" numberOfMessages receivedMessages.Count
                for expectedMsg in expectedMessages do
                    Expect.isTrue "" (receivedMessages.Contains(expectedMsg))
                
                cts.Dispose()
            with
            | :? OperationCanceledException
            | :? TaskCanceledException ->
                cts.Dispose()
                let errorMsg = $"Test timeout: Only received {receivedMessages.Count} out of {numberOfMessages} messages within 30 seconds"
                Log.Error(errorMsg)
                failwith errorMsg
            | ex ->
                cts.Dispose()
                raise ex
            
            Log.Debug("Finished Seek won't get stuck at the receive in MultiTopicsConsumer")
        }
       
    ]
