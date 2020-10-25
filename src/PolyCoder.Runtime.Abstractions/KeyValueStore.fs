namespace PolyCoder.Runtime.KVStorage

open PolyCoder
open PolyCoder.Extra
open FSharp.Control

// Sinks

type KeyValueListerCommand<'k> =
  | ListAllKeysCommand of AsyncFn<'k seq option, unit>
  | ListKeysCommand of 'k * AsyncFn<'k seq option, unit>

type KeyValueFetcherCommand<'k, 'm, 'v> =
  | ExistsKeyCommand of 'k * ResultSink<bool>
  | FetchMetadataCommand of 'k * ResultSink<'m option>
  | FetchValueCommand of 'k * ResultSink<('m * 'v) option>

type KeyValueStorerCommand<'k, 'm, 'v> =
  | StoreValueCommand of 'k * 'm * 'v * ResultSink<unit>
  | UpdateMetadataCommand of 'k * 'm * ResultSink<unit>
  | UpdateValueCommand of 'k * 'v * ResultSink<unit>
  | RemoveKeyCommand of 'k * ResultSink<unit>

type KeyValueCleanerCommand<'k> =
  | ClearKeysCommand of 'k * ResultSink<unit>
  | ClearAllKeysCommand of ResultSink<unit>

type KeyValueStoreCommand<'k, 'm, 'v> =
  | ListerCommand of KeyValueListerCommand<'k>
  | FetcherCommand of KeyValueFetcherCommand<'k, 'm, 'v>
  | StorerCommand of KeyValueStorerCommand<'k, 'm, 'v>
  | CleanerCommand of KeyValueCleanerCommand<'k>

type KeyValueListerSink<'k> = Sink<KeyValueListerCommand<'k>>
type KeyValueFetcherSink<'k, 'm, 'v> = Sink<KeyValueFetcherCommand<'k, 'm, 'v>>
type KeyValueStorerSink<'k, 'm, 'v> = Sink<KeyValueStorerCommand<'k, 'm, 'v>>
type KeyValueCleanerSink<'k> = Sink<KeyValueCleanerCommand<'k>>
type KeyValueStoreSink<'k, 'm, 'v> = Sink<KeyValueStoreCommand<'k, 'm, 'v>>

// Records

type KeyValueLister<'k> = {
  listAllKeys: unit -> AsyncSeq<'k seq>
  listKeys: 'k -> AsyncSeq<'k seq>
}

type KeyValueFetcher<'k, 'm, 'v> = {
  existsKey: 'k -> Async<bool>
  fetchMetadata: 'k -> Async<'m option>
  fetchValue: 'k -> Async<('m * 'v) option>
}

type KeyValueStorer<'k, 'm, 'v> = {
  storeValue: 'k -> 'm -> 'v -> Async<unit>
  updateMetadata: 'k -> 'm -> Async<unit>
  updateValue: 'k -> 'v -> Async<unit>
  removeKey: 'k -> Async<unit>
}

type KeyValueCleaner<'k> = {
  clearKeys: 'k -> Async<unit>
  clearAllKeys: unit -> Async<unit>
}

type KeyValueStore<'k, 'm, 'v> = {
  listAllKeys: unit -> AsyncSeq<'k seq>
  listKeys: 'k -> AsyncSeq<'k seq>
  existsKey: 'k -> Async<bool>
  fetchMetadata: 'k -> Async<'m option>
  fetchValue: 'k -> Async<('m * 'v) option>
  storeValue: 'k -> 'm -> 'v -> Async<unit>
  updateMetadata: 'k -> 'm -> Async<unit>
  updateValue: 'k -> 'v -> Async<unit>
  removeKey: 'k -> Async<unit>
  clearKeys: 'k -> Async<unit>
  clearAllKeys: unit -> Async<unit>
}

// Interfaces
  
type IKeyValueLister<'k> =
  abstract listAllKeys: unit -> AsyncSeq<'k seq>
  abstract listKeys: 'k -> AsyncSeq<'k seq>

type IKeyValueFetcher<'k, 'm, 'v> =
  abstract existsKey: 'k -> Async<bool>
  abstract fetchMetadata: 'k -> Async<'m option>
  abstract fetchValue: 'k -> Async<('m * 'v) option>

type IKeyValueStorer<'k, 'm, 'v> =
  abstract storeValue: 'k * 'm * 'v -> Async<unit>
  abstract updateMetadata: 'k * 'm -> Async<unit>
  abstract updateValue: 'k * 'v -> Async<unit>
  abstract removeKey: 'k -> Async<unit>

type IKeyValueCleaner<'k> =
  abstract clearKeys: 'k -> Async<unit>
  abstract clearAllKeys: unit -> Async<unit>

type IKeyValueStore<'k, 'm, 'v> =
  inherit IKeyValueLister<'k>
  inherit IKeyValueFetcher<'k, 'm, 'v>
  inherit IKeyValueStorer<'k, 'm, 'v>
  inherit IKeyValueCleaner<'k>

// Lister Modules

module KeyValueLister =
  let internal mapsOut fnKeyOut (keys: 'a seq) =
    keys
    |> Seq.map fnKeyOut
    |> AsyncSeq.ofSeqAsync
    |> AsyncSeq.toArrayAsync
    |> Async.map Array.toSeq

  let ofSink ignoreFullBuffer (keyValueListerSink: KeyValueListerSink<'k>) : KeyValueLister<'k> =
    { 
      listAllKeys = fun () ->
        AsyncSeq.ofAsyncFnOption ignoreFullBuffer (fun sink -> keyValueListerSink(ListAllKeysCommand(sink)))
      listKeys = fun key ->
        AsyncSeq.ofAsyncFnOption ignoreFullBuffer (fun sink -> keyValueListerSink(ListKeysCommand(key, sink)))
    }

  let toSink (keyValueLister: KeyValueLister<'k>) : KeyValueListerSink<'k> =
    function
    | ListAllKeysCommand(sink) ->
      keyValueLister.listAllKeys()
      |> AsyncSeq.toAsyncSinkOption sink
      |> Async.Start

    | ListKeysCommand(key, sink) ->
      keyValueLister.listKeys key
      |> AsyncSeq.toAsyncSinkOption sink
      |> Async.Start

  let ofInterface (keyValueLister: IKeyValueLister<'k>) : KeyValueLister<'k> =
    { 
      listAllKeys = fun () -> keyValueLister.listAllKeys()
      listKeys = fun key -> keyValueLister.listKeys key
    }

  let toInterface (keyValueLister: KeyValueLister<'k>) : IKeyValueLister<'k> =
    { new IKeyValueLister<'k> with
        member this.listAllKeys () = keyValueLister.listAllKeys()
        member this.listKeys key = keyValueLister.listKeys key
    }

  let bind fnKeyIn fnKeyOut (lister: KeyValueLister<'a>) : KeyValueLister<'b> =
    { 
      listAllKeys = fun () ->
        lister.listAllKeys()
        |> AsyncSeq.mapAsync (mapsOut fnKeyOut)
      
      listKeys = fun key ->
        AsyncSeq.singleton key
        |> AsyncSeq.mapAsync fnKeyIn
        |> AsyncSeq.collect lister.listKeys
        |> AsyncSeq.mapAsync (mapsOut fnKeyOut)
    }

  let map fnKeyIn fnKeyOut = bind (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)

module IKeyValueLister =
  let ofRecord = KeyValueLister.toInterface
  let toRecord = KeyValueLister.ofInterface

  let ofSink ignoreFullBuffer = 
    KeyValueLister.ofSink ignoreFullBuffer >> KeyValueLister.toInterface

  let toSink<'k> : (IKeyValueLister<'k> -> KeyValueListerSink<'k>) =
    KeyValueLister.ofInterface >> KeyValueLister.toSink

  let bind fnKeyIn fnKeyOut = toRecord >> KeyValueLister.bind fnKeyIn fnKeyOut >> ofRecord

  let map fnKeyIn fnKeyOut = bind (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)

module KeyValueListerSink =
  let ofRecord = KeyValueLister.toSink
  let toRecord = KeyValueLister.ofSink
  let ofInterface<'k> = IKeyValueLister.toSink<'k>
  let toInterface = IKeyValueLister.ofSink

  let bind ignoreFullBuffer fnKeyIn fnKeyOut =
    toRecord ignoreFullBuffer >> KeyValueLister.bind fnKeyIn fnKeyOut >> ofRecord

  let map ignoreFullBuffer fnKeyIn fnKeyOut =
    bind ignoreFullBuffer (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)

// Fetcher Modules

module KeyValueFetcher =
  let ofSink (keyValueFetcherSink: KeyValueFetcherSink<'k, 'm, 'v>) : KeyValueFetcher<'k, 'm, 'v> =
    { 
      existsKey = fun key ->
        ResultSink.toAsync (fun sink -> keyValueFetcherSink(ExistsKeyCommand(key, sink)))
      fetchMetadata = fun key ->
        ResultSink.toAsync (fun sink -> keyValueFetcherSink(FetchMetadataCommand(key, sink)))
      fetchValue = fun key ->
        ResultSink.toAsync (fun sink -> keyValueFetcherSink(FetchValueCommand(key, sink)))
    }

  let toSink (keyValueFetcher: KeyValueFetcher<'k, 'm, 'v>) : KeyValueFetcherSink<'k, 'm, 'v> =
    function
    | ExistsKeyCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueFetcher.existsKey key)

    | FetchMetadataCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueFetcher.fetchMetadata key)

    | FetchValueCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueFetcher.fetchValue key)

  let ofInterface (keyValueFetcher: IKeyValueFetcher<'k, 'm, 'v>) : KeyValueFetcher<'k, 'm, 'v> =
    { 
      existsKey = fun key -> keyValueFetcher.existsKey key
      fetchMetadata = fun key -> keyValueFetcher.fetchMetadata key
      fetchValue = fun key -> keyValueFetcher.fetchValue key
    }

  let toInterface (keyValueFetcher: KeyValueFetcher<'k, 'm, 'v>) : IKeyValueFetcher<'k, 'm, 'v> =
    { new IKeyValueFetcher<'k, 'm, 'v> with
        member this.existsKey key = keyValueFetcher.existsKey key
        member this.fetchMetadata key = keyValueFetcher.fetchMetadata key
        member this.fetchValue key = keyValueFetcher.fetchValue key
    }

  let bind fnKeyIn fnMetaOut fnValueOut 
      (fetcher: KeyValueFetcher<'keyIn, 'metaIn, 'valueIn>) 
      : KeyValueFetcher<'keyOut, 'metaOut, 'valueOut> =
    { 
      existsKey = fun key -> async {
        let! key' = fnKeyIn key
        return! fetcher.existsKey key'
      }
      
      fetchMetadata = fun keyOut -> async {
        let! keyIn = fnKeyIn keyOut
        match! fetcher.fetchMetadata keyIn with
        | Some metaIn ->
          let! metaOut = fnMetaOut metaIn
          return Some metaOut
        | None ->
          return None
      }
      fetchValue = fun keyOut -> async {
        let! keyIn = fnKeyIn keyOut
        match! fetcher.fetchValue keyIn with
        | Some (metaIn, valueIn) ->
          let! metaOut = fnMetaOut metaIn
          let! valueOut = fnValueOut valueIn
          return Some (metaOut, valueOut)
        | None ->
          return None
      }
    }

  let map fnKeyIn fnMetaOut fnValueOut =
    bind (fnKeyIn >> Async.result) (fnMetaOut >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaOut = bind Async.result fnMetaOut Async.result
  let mapMeta fnMetaOut = map id fnMetaOut id

  let bindValue fnValueOut = bind Async.result Async.result fnValueOut
  let mapValue fnValueOut = map id id fnValueOut
  
module IKeyValueFetcher =
  let ofRecord = KeyValueFetcher.toInterface
  let toRecord = KeyValueFetcher.ofInterface

  let ofSink<'k, 'm, 'v> : (KeyValueFetcherSink<'k, 'm, 'v> -> IKeyValueFetcher<'k, 'm, 'v>) = 
    KeyValueFetcher.ofSink >> KeyValueFetcher.toInterface

  let toSink<'k, 'm, 'v> : (IKeyValueFetcher<'k, 'm, 'v> -> KeyValueFetcherSink<'k, 'm, 'v>) =
    KeyValueFetcher.ofInterface >> KeyValueFetcher.toSink

  let bind fnKeyIn fnMetaOut fnValueOut =
    toRecord >> KeyValueFetcher.bind fnKeyIn fnMetaOut fnValueOut >> ofRecord

  let map fnKeyIn fnMetaOut fnValueOut =
    bind (fnKeyIn >> Async.result) (fnMetaOut >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaOut = bind Async.result fnMetaOut Async.result
  let mapMeta fnMetaOut = map id fnMetaOut id

  let bindValue fnValueOut = bind Async.result Async.result fnValueOut
  let mapValue fnValueOut = map id id fnValueOut
  
module KeyValueFetcherSink =
  let ofRecord = KeyValueFetcher.toSink
  let toRecord = KeyValueFetcher.ofSink

  let ofInterface<'k, 'm, 'v> : (IKeyValueFetcher<'k, 'm, 'v> -> KeyValueFetcherSink<'k, 'm, 'v>) = 
    KeyValueFetcher.ofInterface >> KeyValueFetcher.toSink

  let toInterface<'k, 'm, 'v> : (KeyValueFetcherSink<'k, 'm, 'v> -> IKeyValueFetcher<'k, 'm, 'v>) =
    KeyValueFetcher.ofSink >> KeyValueFetcher.toInterface

  let bind fnKeyIn fnMetaOut fnValueOut =
    toRecord >> KeyValueFetcher.bind fnKeyIn fnMetaOut fnValueOut >> ofRecord

  let map fnKeyIn fnMetaOut fnValueOut =
    bind (fnKeyIn >> Async.result) (fnMetaOut >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaOut = bind Async.result fnMetaOut Async.result
  let mapMeta fnMetaOut = map id fnMetaOut id

  let bindValue fnValueOut = bind Async.result Async.result fnValueOut
  let mapValue fnValueOut = map id id fnValueOut

// Storer Modules

module KeyValueStorer =
  let ofSink (keyValueStorerSink: KeyValueStorerSink<'k, 'm, 'v>) : KeyValueStorer<'k, 'm, 'v> =
    { 
      storeValue = fun key meta value ->
        ResultSink.toAsync (fun sink -> keyValueStorerSink(StoreValueCommand(key, meta, value, sink)))
      updateMetadata = fun key meta ->
        ResultSink.toAsync (fun sink -> keyValueStorerSink(UpdateMetadataCommand(key, meta, sink)))
      updateValue = fun key value ->
        ResultSink.toAsync (fun sink -> keyValueStorerSink(UpdateValueCommand(key, value, sink)))
      removeKey = fun key ->
        ResultSink.toAsync (fun sink -> keyValueStorerSink(RemoveKeyCommand(key, sink)))
    }

  let toSink (keyValueStorer: KeyValueStorer<'k, 'm, 'v>) : KeyValueStorerSink<'k, 'm, 'v> =
    function
    | StoreValueCommand(key, meta, value, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueStorer.storeValue key meta value)

    | UpdateMetadataCommand(key, meta, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueStorer.updateMetadata key meta)

    | UpdateValueCommand(key, value, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueStorer.updateValue key value)

    | RemoveKeyCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueStorer.removeKey key)

  let ofInterface (keyValueStorer: IKeyValueStorer<'k, 'm, 'v>) : KeyValueStorer<'k, 'm, 'v> =
    { 
      storeValue = fun key meta value -> keyValueStorer.storeValue(key, meta, value)
      updateMetadata = fun key meta -> keyValueStorer.updateMetadata(key, meta)
      updateValue = fun key value -> keyValueStorer.updateValue(key, value)
      removeKey = fun key -> keyValueStorer.removeKey(key)
    }

  let toInterface (keyValueStorer: KeyValueStorer<'k, 'm, 'v>) : IKeyValueStorer<'k, 'm, 'v> =
    { new IKeyValueStorer<'k, 'm, 'v> with
        member this.storeValue(key, meta, value) =
          keyValueStorer.storeValue key meta value
        member this.updateMetadata(key, meta) =
          keyValueStorer.updateMetadata key meta
        member this.updateValue(key, value) =
          keyValueStorer.updateValue key value
        member this.removeKey(key) =
          keyValueStorer.removeKey key
    }

  let bind fnKeyIn fnMetaIn fnValueIn
      (storer: KeyValueStorer<'keyIn, 'metaIn, 'valueIn>) 
      : KeyValueStorer<'keyOut, 'metaOut, 'valueOut> =
    { 
      storeValue = fun keyOut metaOut valueOut -> async {
        let! keyIn = fnKeyIn keyOut
        let! metaIn = fnMetaIn metaOut
        let! valueIn = fnValueIn valueOut
        do! storer.storeValue keyIn metaIn valueIn
      }
      updateMetadata = fun keyOut metaOut -> async {
        let! keyIn = fnKeyIn keyOut
        let! metaIn = fnMetaIn metaOut
        do! storer.updateMetadata keyIn metaIn
      }
      updateValue = fun keyOut valueOut -> async {
        let! keyIn = fnKeyIn keyOut
        let! valueIn = fnValueIn valueOut
        do! storer.updateValue keyIn valueIn
      }
      removeKey = fun keyOut -> async {
        let! keyIn = fnKeyIn keyOut
        do! storer.removeKey keyIn
      }
    }

  let map fnKeyIn fnMetaIn fnValueIn =
    bind (fnKeyIn >> Async.result) (fnMetaIn >> Async.result) (fnValueIn >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaIn = bind Async.result fnMetaIn Async.result
  let mapMeta fnMetaIn = map id fnMetaIn id

  let bindValue fnValueIn = bind Async.result Async.result fnValueIn
  let mapValue fnValueIn = map id id fnValueIn
  
module IKeyValueStorer =
  let ofRecord = KeyValueStorer.toInterface
  let toRecord = KeyValueStorer.ofInterface

  let ofSink<'k, 'm, 'v> : (KeyValueStorerSink<'k, 'm, 'v> -> IKeyValueStorer<'k, 'm, 'v>) = 
    KeyValueStorer.ofSink >> KeyValueStorer.toInterface

  let toSink<'k, 'm, 'v> : (IKeyValueStorer<'k, 'm, 'v> -> KeyValueStorerSink<'k, 'm, 'v>) =
    KeyValueStorer.ofInterface >> KeyValueStorer.toSink

  let bind fnKeyIn fnMetaOut fnValueOut =
    toRecord >> KeyValueStorer.bind fnKeyIn fnMetaOut fnValueOut >> ofRecord

  let map fnKeyIn fnMetaOut fnValueOut =
    bind (fnKeyIn >> Async.result) (fnMetaOut >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaIn = bind Async.result fnMetaIn Async.result
  let mapMeta fnMetaIn = map id fnMetaIn id

  let bindValue fnValueIn = bind Async.result Async.result fnValueIn
  let mapValue fnValueIn = map id id fnValueIn
  
module KeyValueStorerSink =
  let ofRecord = KeyValueStorer.toSink
  let toRecord = KeyValueStorer.ofSink

  let ofInterface<'k, 'm, 'v> : (IKeyValueStorer<'k, 'm, 'v> -> KeyValueStorerSink<'k, 'm, 'v>) = 
    KeyValueStorer.ofInterface >> KeyValueStorer.toSink

  let toInterface<'k, 'm, 'v> : (KeyValueStorerSink<'k, 'm, 'v> -> IKeyValueStorer<'k, 'm, 'v>) =
    KeyValueStorer.ofSink >> KeyValueStorer.toInterface

  let bind fnKeyIn fnMetaOut fnValueOut =
    toRecord >> KeyValueStorer.bind fnKeyIn fnMetaOut fnValueOut >> ofRecord

  let map fnKeyIn fnMetaOut fnValueOut =
    bind (fnKeyIn >> Async.result) (fnMetaOut >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn = bind fnKeyIn Async.result Async.result
  let mapKey fnKeyIn = map fnKeyIn id id

  let bindMeta fnMetaIn = bind Async.result fnMetaIn Async.result
  let mapMeta fnMetaIn = map id fnMetaIn id

  let bindValue fnValueIn = bind Async.result Async.result fnValueIn
  let mapValue fnValueIn = map id id fnValueIn

// Cleaner Modules

module KeyValueCleaner =
  let ofSink (keyValueCleanerSink: KeyValueCleanerSink<'k>) : KeyValueCleaner<'k> =
    { 
      clearAllKeys = fun () ->
        ResultSink.toAsync (fun sink -> keyValueCleanerSink(ClearAllKeysCommand(sink)))
      clearKeys = fun key ->
        ResultSink.toAsync (fun sink -> keyValueCleanerSink(ClearKeysCommand(key, sink)))
    }

  let toSink (keyValueCleaner: KeyValueCleaner<'k>) : KeyValueCleanerSink<'k> =
    function
    | ClearKeysCommand(key, sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueCleaner.clearKeys key)

    | ClearAllKeysCommand(sink) -> 
      ResultSink.ofAsync sink (fun () -> keyValueCleaner.clearAllKeys())

  let ofInterface (keyValueCleaner: IKeyValueCleaner<'k>) : KeyValueCleaner<'k> =
    { 
      clearAllKeys = fun () -> keyValueCleaner.clearAllKeys()
      clearKeys = fun key -> keyValueCleaner.clearKeys key
    }

  let toInterface (keyValueCleaner: KeyValueCleaner<'k>) : IKeyValueCleaner<'k> =
    { new IKeyValueCleaner<'k> with
        member this.clearAllKeys () = keyValueCleaner.clearAllKeys()
        member this.clearKeys key = keyValueCleaner.clearKeys key
    }

  let bind fnKeyIn (cleaner: KeyValueCleaner<'a>) : KeyValueCleaner<'b> =
    { 
      clearAllKeys = fun () -> cleaner.clearAllKeys()
      clearKeys = fun key -> async {
        let! key' = fnKeyIn key
        return! cleaner.clearKeys key'
      }
    }

  let map fnKeyIn = bind (fnKeyIn >> Async.result)
  
module IKeyValueCleaner =
  let ofRecord = KeyValueCleaner.toInterface
  let toRecord = KeyValueCleaner.ofInterface

  let ofSink<'k> : (KeyValueCleanerSink<'k> -> IKeyValueCleaner<'k>) = 
    KeyValueCleaner.ofSink >> KeyValueCleaner.toInterface

  let toSink<'k> : (IKeyValueCleaner<'k> -> KeyValueCleanerSink<'k>) =
    KeyValueCleaner.ofInterface >> KeyValueCleaner.toSink

  let bind fnKeyIn =
    toRecord
    >> KeyValueCleaner.bind fnKeyIn
    >> ofRecord

  let map fnKeyIn =
    bind (fnKeyIn >> Async.result)
  
module KeyValueCleanerSink =
  let ofRecord = KeyValueCleaner.toSink
  let toRecord = KeyValueCleaner.ofSink

  let ofInterface<'k> : (IKeyValueCleaner<'k> -> KeyValueCleanerSink<'k>) = 
    KeyValueCleaner.ofInterface >> KeyValueCleaner.toSink

  let toInterface<'k> : (KeyValueCleanerSink<'k> -> IKeyValueCleaner<'k>) =
    KeyValueCleaner.ofSink >> KeyValueCleaner.toInterface

  let bind fnKeyIn = toRecord >> KeyValueCleaner.bind fnKeyIn >> ofRecord

  let map fnKeyIn = bind (fnKeyIn >> Async.result)

// Store Modules

module KeyValueStore =
  let combine
      (lister: KeyValueLister<'k>)
      (fetcher: KeyValueFetcher<'k, 'm, 'v>)
      (storer: KeyValueStorer<'k, 'm, 'v>)
      (cleaner: KeyValueCleaner<'k>)
      : KeyValueStore<'k, 'm, 'v> =
    {
      listAllKeys = lister.listAllKeys
      listKeys = lister.listKeys
      existsKey = fetcher.existsKey
      fetchMetadata = fetcher.fetchMetadata
      fetchValue = fetcher.fetchValue
      storeValue = storer.storeValue
      updateMetadata = storer.updateMetadata
      updateValue = storer.updateValue
      removeKey = storer.removeKey
      clearKeys = cleaner.clearKeys
      clearAllKeys = cleaner.clearAllKeys
    }

  let extractLister (store: KeyValueStore<'k, _, _>) : KeyValueLister<'k> =
    {
      listAllKeys = store.listAllKeys
      listKeys = store.listKeys
    }

  let extractFetcher (store: KeyValueStore<'k, 'm, 'v>) : KeyValueFetcher<'k, 'm, 'v> =
    {
      existsKey = store.existsKey
      fetchMetadata = store.fetchMetadata
      fetchValue = store.fetchValue
    }

  let extractStorer (store: KeyValueStore<'k, 'm, 'v>) : KeyValueStorer<'k, 'm, 'v> =
    {
      storeValue = store.storeValue
      updateMetadata = store.updateMetadata
      updateValue = store.updateValue
      removeKey = store.removeKey
    }

  let extractCleaner (store: KeyValueStore<'k, _, _>) : KeyValueCleaner<'k> =
    {
      clearKeys = store.clearKeys
      clearAllKeys = store.clearAllKeys
    }

  let extract store =
    extractLister store,
    extractFetcher store,
    extractStorer store,
    extractCleaner store


  let internal combineInterfaces
      (lister: IKeyValueLister<'k>)
      (fetcher: IKeyValueFetcher<'k, 'm, 'v>)
      (storer: IKeyValueStorer<'k, 'm, 'v>)
      (cleaner: IKeyValueCleaner<'k>)
      : IKeyValueStore<'k, 'm, 'v> =
    { new IKeyValueStore<'k, 'm, 'v> with
      member _.listAllKeys() = lister.listAllKeys()
      member _.listKeys(key) = lister.listKeys(key)
      member _.existsKey(key) = fetcher.existsKey(key)
      member _.fetchMetadata(key) = fetcher.fetchMetadata(key)
      member _.fetchValue(key) = fetcher.fetchValue(key)
      member _.storeValue(key, meta, value) = storer.storeValue(key, meta, value)
      member _.updateMetadata(key, meta) = storer.updateMetadata(key, meta)
      member _.updateValue(key, value) = storer.updateValue(key, value)
      member _.removeKey(key) = storer.removeKey(key)
      member _.clearKeys(key) = cleaner.clearKeys(key)
      member _.clearAllKeys() = cleaner.clearAllKeys()
    }

  let internal extractListerInterface (store: IKeyValueStore<'k, _, _>) : IKeyValueLister<'k> =
    { new IKeyValueLister<'k> with
      member _.listAllKeys() = store.listAllKeys()
      member _.listKeys(key) = store.listKeys(key)
    }

  let internal extractFetcherInterface (store: IKeyValueStore<'k, 'm, 'v>) : IKeyValueFetcher<'k, 'm, 'v> =
    { new IKeyValueFetcher<'k, 'm, 'v> with
      member _.existsKey(key) = store.existsKey(key)
      member _.fetchMetadata(key) = store.fetchMetadata(key)
      member _.fetchValue(key) = store.fetchValue(key)
    }

  let internal extractStorerInterface (store: IKeyValueStore<'k, 'm, 'v>) : IKeyValueStorer<'k, 'm, 'v> =
    { new IKeyValueStorer<'k, 'm, 'v> with
      member _.storeValue(key, meta, value) = store.storeValue(key, meta, value)
      member _.updateMetadata(key, meta) = store.updateMetadata(key, meta)
      member _.updateValue(key, value) = store.updateValue(key, value)
      member _.removeKey(key) = store.removeKey(key)
    }

  let internal extractCleanerInterface (store: IKeyValueStore<'k, _, _>) : IKeyValueCleaner<'k> =
    { new IKeyValueCleaner<'k> with
      member _.clearKeys(key) = store.clearKeys(key)
      member _.clearAllKeys() = store.clearAllKeys()
    }

  let internal extractInterfaces store =
    extractListerInterface store,
    extractFetcherInterface store,
    extractStorerInterface store,
    extractCleanerInterface store


  let internal combineSinks
      (lister: KeyValueListerSink<'k>)
      (fetcher: KeyValueFetcherSink<'k, 'm, 'v>)
      (storer: KeyValueStorerSink<'k, 'm, 'v>)
      (cleaner: KeyValueCleanerSink<'k>)
      : KeyValueStoreSink<'k, 'm, 'v> =
    function
    | ListerCommand command -> lister(command)
    | FetcherCommand command -> fetcher(command)
    | StorerCommand command -> storer(command)
    | CleanerCommand command -> cleaner(command)

  let internal extractListerSink (store: KeyValueStoreSink<'k, _, _>) : KeyValueListerSink<'k> =
    ListerCommand >> store

  let internal extractFetcherSink (store: KeyValueStoreSink<'k, 'm, 'v>) : KeyValueFetcherSink<'k, 'm, 'v> =
    FetcherCommand >> store

  let internal extractStorerSink (store: KeyValueStoreSink<'k, 'm, 'v>) : KeyValueStorerSink<'k, 'm, 'v> =
    StorerCommand >> store

  let internal extractCleanerSink (store: KeyValueStoreSink<'k, _, _>) : KeyValueCleanerSink<'k> =
    CleanerCommand >> store

  let internal extractSinks store =
    extractListerSink store,
    extractFetcherSink store,
    extractStorerSink store,
    extractCleanerSink store


  let ofSink 
      ignoreFullBuffer 
      (keyValueStoreSink: KeyValueStoreSink<'k, 'm, 'v>) 
      : KeyValueStore<'k, 'm, 'v> =
    let lister, fetcher, storer, cleaner =
      extractSinks keyValueStoreSink

    combine
      (KeyValueLister.ofSink ignoreFullBuffer lister)
      (KeyValueFetcher.ofSink fetcher)
      (KeyValueStorer.ofSink storer)
      (KeyValueCleaner.ofSink cleaner)

  let toSink
      (keyValueStore: KeyValueStore<'k, 'm, 'v>) 
      : KeyValueStoreSink<'k, 'm, 'v> =
    let lister, fetcher, storer, cleaner =
      extract keyValueStore

    combineSinks
      (KeyValueLister.toSink lister)
      (KeyValueFetcher.toSink fetcher)
      (KeyValueStorer.toSink storer)
      (KeyValueCleaner.toSink cleaner)
    
  let ofInterface (keyValueStore: IKeyValueStore<'k, 'm, 'v>) : KeyValueStore<'k, 'm, 'v> =
    let lister, fetcher, storer, cleaner =
      extractInterfaces keyValueStore

    combine
      (KeyValueLister.ofInterface lister)
      (KeyValueFetcher.ofInterface fetcher)
      (KeyValueStorer.ofInterface storer)
      (KeyValueCleaner.ofInterface cleaner)

  let toInterface (keyValueStore: KeyValueStore<'k, 'm, 'v>) : IKeyValueStore<'k, 'm, 'v> =
    let lister, fetcher, storer, cleaner =
      extract keyValueStore

    combineInterfaces
      (KeyValueLister.toInterface lister)
      (KeyValueFetcher.toInterface fetcher)
      (KeyValueStorer.toInterface storer)
      (KeyValueCleaner.toInterface cleaner)

  let bind fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut
      (store: KeyValueStore<'keyIn, 'metaIn, 'valueIn>) 
      : KeyValueStore<'keyOut, 'metaOut, 'valueOut> =
    let lister, fetcher, storer, cleaner =
      extract store

    combine
      (KeyValueLister.bind fnKeyIn fnKeyOut lister)
      (KeyValueFetcher.bind fnKeyIn fnMetaOut fnValueOut fetcher)
      (KeyValueStorer.bind fnKeyIn fnMetaIn fnValueIn storer)
      (KeyValueCleaner.bind fnKeyIn cleaner)

  let map fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut =
    bind
      (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)
      (fnMetaIn >> Async.result) (fnMetaOut >> Async.result)
      (fnValueIn >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn fnKeyOut = bind fnKeyIn fnKeyOut Async.result Async.result Async.result Async.result
  let mapKey fnKeyIn fnKeyOut = map fnKeyIn fnKeyOut id id id id

  let bindMeta fnMetaIn fnMetaOut = bind Async.result Async.result fnMetaIn fnMetaOut Async.result Async.result
  let mapMeta fnMetaIn fnMetaOut = map id id fnMetaIn fnMetaOut id id

  let bindValue fnValueIn fnValueOut = bind Async.result Async.result Async.result Async.result fnValueIn fnValueOut
  let mapValue fnValueIn fnValueOut = map id id id id fnValueIn fnValueOut
  
module IKeyValueStore =
  let combine = KeyValueStore.combineInterfaces

  let extractLister = KeyValueStore.extractListerInterface
  let extractFetcher = KeyValueStore.extractFetcherInterface
  let extractStorer = KeyValueStore.extractStorerInterface
  let extractCleaner = KeyValueStore.extractCleanerInterface
  
  let extract = KeyValueStore.extractInterfaces


  let ofRecord = KeyValueStore.toInterface
  let toRecord = KeyValueStore.ofInterface

  let ofSink ignoreFullBuffer = 
    KeyValueStore.ofSink ignoreFullBuffer >> KeyValueStore.toInterface

  let toSink<'k, 'm, 'v> : (IKeyValueStore<'k, 'm, 'v> -> KeyValueStoreSink<'k, 'm, 'v>) =
    KeyValueStore.ofInterface >> KeyValueStore.toSink

  let bind fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut =
    toRecord 
    >> KeyValueStore.bind fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut
    >> ofRecord

  let map fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut =
    bind 
      (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)
      (fnMetaIn >> Async.result) (fnMetaOut >> Async.result)
      (fnValueIn >> Async.result) (fnValueOut >> Async.result)

  let bindKey fnKeyIn fnKeyOut = bind fnKeyIn fnKeyOut Async.result Async.result Async.result Async.result
  let mapKey fnKeyIn fnKeyOut = map fnKeyIn fnKeyOut id id id id

  let bindMeta fnMetaIn fnMetaOut = bind Async.result Async.result fnMetaIn fnMetaOut Async.result Async.result
  let mapMeta fnMetaIn fnMetaOut = map id id fnMetaIn fnMetaOut id id

  let bindValue fnValueIn fnValueOut = bind Async.result Async.result Async.result Async.result fnValueIn fnValueOut
  let mapValue fnValueIn fnValueOut = map id id id id fnValueIn fnValueOut
  
module KeyValueStoreSink =
  let combine = KeyValueStore.combineSinks

  let extractLister = KeyValueStore.extractListerSink
  let extractFetcher = KeyValueStore.extractFetcherSink
  let extractStorer = KeyValueStore.extractStorerSink
  let extractCleaner = KeyValueStore.extractCleanerSink
  
  let extract = KeyValueStore.extractSinks


  let ofRecord = KeyValueStore.toSink
  let toRecord = KeyValueStore.ofSink

  let ofInterface<'k, 'm, 'v> : (IKeyValueStore<'k, 'm, 'v> -> KeyValueStoreSink<'k, 'm, 'v>) = 
    KeyValueStore.ofInterface >> KeyValueStore.toSink

  let toInterface ignoreFullBuffer =
    KeyValueStore.ofSink ignoreFullBuffer >> KeyValueStore.toInterface

  let bind ignoreFullBuffer fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut =
    toRecord ignoreFullBuffer
    >> KeyValueStore.bind fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut 
    >> ofRecord

  let map ignoreFullBuffer fnKeyIn fnKeyOut fnMetaIn fnMetaOut fnValueIn fnValueOut =
    bind ignoreFullBuffer
      (fnKeyIn >> Async.result) (fnKeyOut >> Async.result)
      (fnMetaIn >> Async.result) (fnMetaOut >> Async.result)
      (fnValueIn >> Async.result) (fnValueOut >> Async.result)

  let bindKey ignoreFullBuffer fnKeyIn fnKeyOut =
    bind ignoreFullBuffer fnKeyIn fnKeyOut Async.result Async.result Async.result Async.result
  let mapKey ignoreFullBuffer fnKeyIn fnKeyOut =
    map ignoreFullBuffer fnKeyIn fnKeyOut id id id id

  let bindMeta ignoreFullBuffer fnMetaIn fnMetaOut =
    bind ignoreFullBuffer Async.result Async.result fnMetaIn fnMetaOut Async.result Async.result
  let mapMeta ignoreFullBuffer fnMetaIn fnMetaOut =
    map ignoreFullBuffer id id fnMetaIn fnMetaOut id id

  let bindValue ignoreFullBuffer fnValueIn fnValueOut =
    bind ignoreFullBuffer Async.result Async.result Async.result Async.result fnValueIn fnValueOut
  let mapValue ignoreFullBuffer fnValueIn fnValueOut =
    map ignoreFullBuffer id id id id fnValueIn fnValueOut
