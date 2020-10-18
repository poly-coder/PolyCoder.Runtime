namespace PolyCoder.Runtime.Storage

open PolyCoder
open FSharp.Control
open PolyCoder.Extra.Collections

type KeyValueListerCommand<'k> =
  | ListAllKeysCommand of AsyncFn<'k seq option, unit>
  | ListKeysCommand of 'k * AsyncFn<'k seq option, unit>
  | ExistsKeyCommand of 'k * ResultSink<bool>

type KeyValueFetcherCommand<'k, 'v, 'm> =
  | FetchMetadataCommand of 'k * ResultSink<'m option>
  | FetchValueCommand of 'k * ResultSink<('m * 'v) option>

type KeyValueStorerCommand<'k, 'v, 'm> =
  | StoreValueCommand of 'k * 'm * 'v * ResultSink<unit>
  | UpdateMetadataCommand of 'k * 'm * ResultSink<unit>
  | UpdateValueCommand of 'k * 'v * ResultSink<unit>
  | RemoveKeyCommand of 'k * ResultSink<unit>

type KeyValueCleanerCommand<'k> =
  | ClearKeysCommand of 'k * ResultSink<unit>
  | ClearAllKeysCommand of ResultSink<unit>

type KeyValueStoreCommand<'k, 'v, 'm> =
  | ListerCommand of KeyValueListerCommand<'k>
  | FetcherCommand of KeyValueFetcherCommand<'k, 'v, 'm>
  | StorerCommand of KeyValueStorerCommand<'k, 'v, 'm>
  | CleanerCommand of KeyValueCleanerCommand<'k>

type KeyValueListerSink<'k> = Sink<KeyValueListerCommand<'k>>
type KeyValueFetcherSink<'k, 'v, 'm> = Sink<KeyValueFetcherCommand<'k, 'v, 'm>>
type KeyValueStorerSink<'k, 'v, 'm> = Sink<KeyValueStorerCommand<'k, 'v, 'm>>
type KeyValueCleanerSink<'k> = Sink<KeyValueCleanerCommand<'k>>
type KeyValueStoreSink<'k, 'v, 'm> = Sink<KeyValueStoreCommand<'k, 'v, 'm>>

type IKeyValueLister<'k> =
  abstract listAllKeys: unit -> AsyncSeq<'k seq>
  abstract listKeys: 'k -> AsyncSeq<'k seq>
  abstract existsKey: 'k -> Async<bool>

type IKeyValueFetcher<'k, 'v, 'm> =
  abstract fetchMetadata: 'k -> Async<'m option>
  abstract fetchValue: 'k -> Async<('m * 'v) option>

type IKeyValueStorer<'k, 'v, 'm> =
  abstract storeValue: 'k -> 'm -> 'v -> Async<unit>
  abstract updateMetadata: 'k -> 'm -> Async<unit>
  abstract updateValue: 'k -> 'v -> Async<unit>
  abstract removeKey: 'k -> Async<unit>

type IKeyValueCleaner<'k> =
  abstract clearKeys: 'k -> Async<unit>
  abstract clearAllKeys: unit -> Async<unit>

type IKeyValueStore<'k, 'v, 'm> =
  inherit IKeyValueLister<'k>
  inherit IKeyValueFetcher<'k, 'v, 'm>
  inherit IKeyValueStorer<'k, 'v, 'm>
  inherit IKeyValueCleaner<'k>

module KeyValueLister =
  let sinkToInterface ignoreFullBuffer (keyValueListerSink: KeyValueListerSink<'k>) : IKeyValueLister<'k> =
    { new IKeyValueLister<'k> with
        member this.listAllKeys () =
          AsyncSeq.ofAsyncFnOption ignoreFullBuffer (fun sink -> keyValueListerSink(ListAllKeysCommand(sink)))

        member this.listKeys key =
          AsyncSeq.ofAsyncFnOption ignoreFullBuffer (fun sink -> keyValueListerSink(ListKeysCommand(key, sink)))

        member this.existsKey key =
          ResultSink.toAsync (fun sink -> keyValueListerSink(ExistsKeyCommand(key, sink)))
    }

  let interfaceToSink (keyValueLister: IKeyValueLister<'k>) : KeyValueListerSink<'k> =
    function
    | ExistsKeyCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueLister.existsKey key)

    | ListAllKeysCommand(sink) ->
      keyValueLister.listAllKeys()
      |> AsyncSeq.toAsyncSinkOption sink
      |> Async.Start

    | ListKeysCommand(key, sink) ->
      keyValueLister.listKeys key
      |> AsyncSeq.toAsyncSinkOption sink
      |> Async.Start

  let bindKey fnIn fnOut (lister: IKeyValueLister<'a>) : IKeyValueLister<'b> =
    { new IKeyValueLister<'b> with
      member _.existsKey key = async {
        let! key' = fnIn key
        return! lister.existsKey key'
      }
      member _.listAllKeys () = 
        lister.listAllKeys()
        |> AsyncSeq.mapAsync fnOut
      member _.listKeys key = 
        AsyncSeq.singleton key
        |> AsyncSeq.mapAsync fnIn
        |> AsyncSeq.collect lister.listKeys
        |> AsyncSeq.mapAsync fnOut
    }


//[<AutoOpen>]
//module Utilities =
//  let sinkToInterface (keyValueStoreSink: KeyValueStoreSink<'k, 'v, 'm>) : IKeyValueStore<'k, 'v, 'm> =
//    { new IKeyValueStore<'k, 'v, 'm> with
//        member this.existsKey key =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(ExistsKeyCommand(key, sink)))

//        member this.fetchMetadata key =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(FetchMetadataCommand(key, sink)))

//        member this.fetchValue key =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(FetchValueCommand(key, sink)))

//        member this.storeValue key metadata value =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(StoreValueCommand(key, metadata, value, sink)))

//        member this.updateMetadata key metadata =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(UpdateMetadataCommand(key, metadata, sink)))

//        member this.updateValue key value =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(UpdateValueCommand(key, value, sink)))

//        member this.removeKey key =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(RemoveKeyCommand(key, sink)))

//        member this.clearAllKeys () =
//          ResultSink.toAsync (fun sink -> keyValueStoreSink(ClearAllKeysCommand(sink)))
//    }

//  let interfaceToSink (keyValueStore: IKeyValueStore<'k, 'v, 'm>) : KeyValueStoreSink<'k, 'v, 'm> =
//    function
//    | ExistsKeyCommand(key, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.existsKey key)

//    | FetchMetadataCommand(key, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.fetchMetadata key)

//    | FetchValueCommand(key, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.fetchValue key)

//    | StoreValueCommand(key, metadata, value, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.storeValue key metadata value)

//    | UpdateMetadataCommand(key, metadata, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.updateMetadata key metadata)

//    | UpdateValueCommand(key, value, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.updateValue key value)

//    | RemoveKeyCommand(key, sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.removeKey key)

//    | ClearAllKeysCommand(sink) -> 
//      ResultSink.ofAsync sink (fun () -> keyValueStore.clearAllKeys ())
