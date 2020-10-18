namespace PolyCoder.Runtime.Storage

type BlobsKeyValueStoreOptions = 
  {
    connectionString: string
  }

type BlobsKeyValueStore(options: BlobsKeyValueStoreOptions) =
  class end
