﻿namespace PolyCoder.Extra

open PolyCoder
open PolyCoder.Extra.Collections
open FSharp.Control

// AsyncSeq

module AsyncSeq =

  type AsyncStepResult<'a> =
    | AsyncValue of 'a
    | AsyncComplete
    | AsyncError of exn

  let ofAsyncGetter ignoreFullBuffer (getter: IAsyncGetter<AsyncStepResult<'a>>) : AsyncSeq<'a> =
    let rec loop() = asyncSeq {
      match! getter.get() with
      | ValueWasGet(AsyncValue(a)) ->
        yield a
        yield! loop()
      | ValueWasGet(AsyncError(exn)) ->
        return raise exn
      | ValueWasGet(AsyncComplete) ->
        return ()
      | GetBufferIsFull ->
        if ignoreFullBuffer then
          yield! loop()
        else
          return invalidOp "Buffer of value getters is full!"
    }
  
    loop()

  let toSink (sink: Sink<AsyncStepResult<'a>>) (source: AsyncSeq<'a>) =
    let enumerator = source.GetEnumerator()

    let rec loop () = async {
      try
        match! enumerator.MoveNext() with
        | Some value ->
          sink(AsyncValue value)
          return! loop()
        | None ->
          enumerator.Dispose();
          sink(AsyncComplete)
      with exn ->
        enumerator.Dispose();
        sink(AsyncError exn)
    }
  
    loop()

  let toAsyncSink (sink: AsyncFn<AsyncStepResult<'a>, unit>) (source: AsyncSeq<'a>) =
    let enumerator = source.GetEnumerator()

    let rec loop () = async {
      try
        match! enumerator.MoveNext() with
        | Some value ->
          do! sink(AsyncValue value)
          return! loop()
        | None ->
          enumerator.Dispose();
          do! sink(AsyncComplete)
      with exn ->
        enumerator.Dispose();
        do! sink(AsyncError exn)
    }
  
    loop()

  let toAsyncPutter ignoreFullBuffer (putter: IAsyncPutter<AsyncStepResult<'a>>) (source: AsyncSeq<'a>) =
    let enumerator = source.GetEnumerator()

    let rec putLoop what = async {
      match! putter.put(what) with
      | PutBufferIsFull ->
        if ignoreFullBuffer then
          do! Async.Sleep 100
          // Try the same value after 100 ms // TODO: add options for this
          return! putLoop what
        else
          return invalidOp "Buffer of value putters is full!"
      | ValueWasPut ->
        return ()
    }

    let rec loop () = async {
      try
        match! enumerator.MoveNext() with
        | Some value ->
          do! putLoop (AsyncValue value)
          return! loop()
        | None ->
          enumerator.Dispose();
          do! putLoop AsyncComplete
      with exn ->
        enumerator.Dispose();
        do! putLoop (AsyncError exn)
    }
  
    loop()

  let ofAsyncFn ignoreFullBuffer (fn: (AsyncFn<AsyncStepResult<'a>, unit>) -> unit) : AsyncSeq<'a> =
    let buffer =
      BufferMailbox.withBufferSize 0
      |> BufferMailbox.create
      |> BufferMailbox.toInterface

    fn(fun result -> async {
      match! buffer.put(result) with
      | PutBufferIsFull when not ignoreFullBuffer ->
        invalidOp "Buffer of value putters is full!"
      | _ -> ()
        
      // TODO: Check if value was put
      return ()
    })

    buffer |> ofAsyncGetter ignoreFullBuffer

  let ofAsyncGetterOption ignoreFullBuffer getter =
    getter
    |> IAsyncGetter.mapValue (function
        | Some v -> AsyncValue v
        | None -> AsyncComplete)
    |> ofAsyncGetter ignoreFullBuffer

  let ofAsyncFnOption ignoreFullBuffer (fn: (AsyncFn<'a option, unit>) -> unit) : AsyncSeq<'a> =
    let buffer =
      BufferMailbox.withBufferSize 0
      |> BufferMailbox.create
      |> BufferMailbox.toInterface

    fn(fun result -> async {
      match! buffer.put(result) with
      | PutBufferIsFull when not ignoreFullBuffer ->
        invalidOp "Buffer of value putters is full!"
      | _ -> ()
       
      // TODO: Check if value was put
      return ()
    })

    buffer |> ofAsyncGetterOption ignoreFullBuffer
 
  let toSinkOption (sink: Sink<'a option>) =
    toSink (function
    | AsyncValue value -> sink(Some value)
    | _ -> sink(None))
 
  let toAsyncSinkOption (sink: AsyncFn<'a option, unit>) =
    toAsyncSink (function
    | AsyncValue value -> sink(Some value)
    | _ -> sink(None))

// RecoverableService

type RecoverableService<'service, 'result> = ('service -> Async<'result>) -> Async<'result>

module RecoverableService =
  let create createService isFatalError : RecoverableService<'service, 'result> =

    let lockObj = obj()
    let mutable serviceTask : Async<'service> option = None

    let get() = async {
      match serviceTask with
      | Some task -> return! task
      | None ->
        let task = lock lockObj (fun () ->
          match serviceTask with
          | Some task -> task
          | None ->
            let task = createService()
            serviceTask <- Some task
            task
        )
        return! task        
    }

    let discard() =
      match serviceTask with
      | Some _ ->
        lock lockObj (fun () ->
          match serviceTask with
          | Some _ ->
            serviceTask <- None
          | None -> ()
        )
      | None -> ()

    let withService fn = async {
      let! service = get()
      try
        let! result = fn(service)
        return result
      with
      | exn when isFatalError exn ->
        discard()
        return Exn.reraise exn
    }

    withService

