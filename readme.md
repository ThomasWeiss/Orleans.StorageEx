# Orleans Extended Table Storage Provider

Stores Orleans grain state in table storage as Bson and splits the binary content over multiple entity properties if the size exceeds 64KB.

## Installation

The Orleans SDK must be installed first.
Build the project. This will copy the Orleans.StorageEx assembly and required references to the Orleans SDK directory.

## Usage

Configuration is similar to the original AzureTableStorage:

```xml
<?xml version="1.0" encoding="utf-8"?>
<OrleansConfiguration xmlns="urn:orleans">
  <Globals>
    <StorageProviders>
      <Provider Type="Orleans.StorageEx.AzureTableStorageEx" Name="AzureStoreEx" DataConnectionString="..."/>
    </StorageProviders>
    ...
```

## Acknowledgements

Post-build events taken from the [OrleansBlobStorageProvider](https://github.com/OrleansContrib/OrleansBlobStorageProvider).

## License

Apache License 2.0