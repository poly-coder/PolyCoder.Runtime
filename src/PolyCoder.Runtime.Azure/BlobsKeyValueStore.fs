namespace PolyCoder.Runtime.KVStorage

open PolyCoder
open PolyCoder.Extra
open Azure
open Azure.Storage.Blobs
open Azure.Storage.Blobs.Models
open System.Collections.Generic
open System
open System.IO

type BlobsKeyValueStoreOptions = 
  {
    connectionString: string
    container: string
    createIfNotExists: bool
    publicAccessType: PublicAccessType
  }

module BlobsKeyValueStore =
  let createSink (options: BlobsKeyValueStoreOptions) : KeyValueStoreSink<string, Map<string, string>, byte[]> =
    let start asyncAction = Async.Start(asyncAction)

    let createContainer () = async {
      let service = BlobServiceClient(options.connectionString)
      let containerClient = service.GetBlobContainerClient(options.container)
      if options.createIfNotExists then
        let! cancellationToken = Async.CancellationToken
        let! _ =
          containerClient.CreateIfNotExistsAsync(options.publicAccessType, cancellationToken = cancellationToken)
          |> Async.AwaitTask
        ()
      return containerClient
    }

    let withContainer =
      RecoverableService.create
        createContainer
        (fun _ -> false) // Consider all exceptions as no affecting the container

    let loopKeys sink (pageEnumerable: IAsyncEnumerable<Page<BlobItem>>) = async {
      let! cancellationToken = Async.CancellationToken

      let pageEnumerator = pageEnumerable.GetAsyncEnumerator(cancellationToken)

      let rec loop () = async {
        let! moved = pageEnumerator.MoveNextAsync().AsTask() |> Async.AwaitTask
        if moved then
          let page = pageEnumerator.Current
          if page.Values.Count > 0 then
            let blobNames =
              page.Values 
              |> Seq.map (fun item -> item.Name)
              |> Seq.cache

            do! blobNames
                |> Some
                |> sink

          return! loop ()

        else
          do! None |> sink
          return ()
      }

      return! loop()
    }

    let deleteItems (container: BlobContainerClient) items = async {
      let! cancellationToken = Async.CancellationToken

      for blobName in items do
        try
          let! _ignore = container.DeleteBlobAsync(blobName, cancellationToken = cancellationToken) |> Async.AwaitTask
          ()
        with
        | :? OperationCanceledException as exn -> Exn.reraise exn
        | _ -> ()
    }

    function
    | ListerCommand(ListAllKeysCommand(sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken

        let pages =
          container
            .GetBlobsAsync(cancellationToken = cancellationToken)
            .AsPages()

        return! loopKeys sink pages
      }) |> start

    | ListerCommand(ListKeysCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken

        let pages = 
          container
            .GetBlobsAsync(prefix = key, cancellationToken = cancellationToken)
            .AsPages()

        return! loopKeys sink pages
      }) |> start

    | CleanerCommand(ClearAllKeysCommand(sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken

        try
          let pages =
            container
              .GetBlobsAsync(cancellationToken = cancellationToken)
              .AsPages()

          do! pages |> loopKeys (fun maybeItems -> async {
            match maybeItems with
            | Some items ->
              do! deleteItems container items
            | None ->
              ()
          })

          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | CleanerCommand(ClearKeysCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken

        try
          let pages =
            container
              .GetBlobsAsync(prefix = key, cancellationToken = cancellationToken)
              .AsPages()

          do! pages |> loopKeys (fun maybeItems -> async {
            match maybeItems with
            | Some items ->
              do! deleteItems container items
            | None ->
              ()
          })

          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | FetcherCommand(ExistsKeyCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          let! response = blob.ExistsAsync(cancellationToken) |> Async.AwaitTask
          let exists = response.Value
          Result.Ok exists |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | FetcherCommand(FetchMetadataCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          let! response = blob.GetPropertiesAsync(cancellationToken = cancellationToken) |> Async.AwaitTask
          let metadata = response.Value.Metadata |> Map.ofDict
          Some metadata |> Result.Ok |> sink
        with
        | :? RequestFailedException as ex when ex.Status = 404 ->
          Result.Ok None |> sink
        | exn -> Result.Error exn |> sink
      }) |> start

    | FetcherCommand(FetchValueCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          let! response = blob.GetPropertiesAsync(cancellationToken = cancellationToken) |> Async.AwaitTask
          let metadata = response.Value.Metadata |> Map.ofDict
          use mem = new MemoryStream()
          let! _ = blob.DownloadToAsync(mem, cancellationToken = cancellationToken) |> Async.AwaitTask
          let bytes = mem.ToArray()
          Some (metadata, bytes) |> Result.Ok |> sink
        with
        | :? RequestFailedException as ex when ex.Status = 404 ->
          Result.Ok None |> sink
        | exn -> Result.Error exn |> sink
      }) |> start

    | StorerCommand(StoreValueCommand(key, metadata, value, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          use mem = new MemoryStream(value)
          let! _response = blob.UploadAsync(mem, metadata = metadata, cancellationToken = cancellationToken) |> Async.AwaitTask
          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | StorerCommand(UpdateMetadataCommand(key, metadata, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          let! _response = blob.SetMetadataAsync(metadata, cancellationToken = cancellationToken) |> Async.AwaitTask
          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | StorerCommand(UpdateValueCommand(key, value, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          use mem = new MemoryStream(value)
          let! _response = blob.UploadAsync(mem, cancellationToken = cancellationToken) |> Async.AwaitTask
          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

    | StorerCommand(RemoveKeyCommand(key, sink)) ->
      withContainer (fun container -> async {
        let! cancellationToken = Async.CancellationToken
        try
          let blob = container.GetBlobClient(key)
          let! _response = blob.DeleteAsync(cancellationToken = cancellationToken) |> Async.AwaitTask
          Result.Ok() |> sink
        with exn -> Result.Error exn |> sink
      }) |> start

  let createInterface options =
    createSink options
    |> IKeyValueStore.ofSink false

  let create options =
    createSink options
    |> KeyValueStore.ofSink false
