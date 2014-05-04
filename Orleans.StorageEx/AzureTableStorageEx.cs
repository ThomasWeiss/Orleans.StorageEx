namespace Orleans.StorageEx
{
	using Microsoft.WindowsAzure.Storage;
	using Microsoft.WindowsAzure.Storage.Table;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Bson;
	using Orleans.Storage;
	using System;
	using System.Collections.Generic;
	using System.Dynamic;
	using System.IO;
	using System.Linq;
	using System.Threading.Tasks;
	
	public class AzureTableStorageEx : IStorageProvider
    {
		public OrleansLogger Log { get; private set; }

		public string Name { get; private set; }

		public Task Init(string name, Providers.IProviderRuntime providerRuntime, Providers.IProviderConfiguration config)
		{
			this.Name = name;
			
			if (!config.Properties.ContainsKey("DataConnectionString") || string.IsNullOrWhiteSpace(config.Properties["DataConnectionString"]))
			{
				throw new ArgumentException("DataConnectionString property not set");
			}
			var connectionString = config.Properties["DataConnectionString"];

			var tableName = "OrleansGrainState";
			if (config.Properties.ContainsKey("TableName"))
			{
				 tableName = config.Properties["TableName"];
			}

			Log = providerRuntime.GetLogger("Storage.AzureTableStorageEx", Logger.LoggerType.Runtime);

			CloudStorageAccount storageAccount = null;
			if (!CloudStorageAccount.TryParse(connectionString, out storageAccount))
			{
				throw new ApplicationException("Invalid DataConnectionString");
			}

			var tableClient = storageAccount.CreateCloudTableClient();
			_table = tableClient.GetTableReference(tableName);

			return _table.CreateIfNotExistsAsync();
		}

		public Task Close()
		{
			return TaskDone.Done;
		}

		public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
		{
			var tableResult = await _table.ExecuteAsync(TableOperation.Retrieve<DynamicTableEntity>(grainReference.ToKeyString(), grainType));
			if (tableResult.Result == null)
			{
				return;
			}
			var entity = tableResult.Result as DynamicTableEntity;

			var serializer = new JsonSerializer();
			using (var memoryStream = new MemoryStream())
			{
				foreach (var propertyName in entity.Properties.Keys.Where(p => p.StartsWith("d")).OrderBy(p => p))
				{
					var dataPart = entity.Properties[propertyName];
					await memoryStream.WriteAsync(dataPart.BinaryValue, 0, dataPart.BinaryValue.Length);
				}

				memoryStream.Position = 0;
				using (var bsonReader = new BsonReader(memoryStream))
				{
					var data = serializer.Deserialize<Dictionary<string, object>>(bsonReader);
					grainState.SetAll(data);
				}
			}
		}

		public Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
		{
			var entity = new DynamicTableEntity(grainReference.ToKeyString(), grainType) { ETag = "*" };

			var serializer = new JsonSerializer();
			using (var memoryStream = new MemoryStream())
			{
				using (var bsonWriter = new BsonWriter(memoryStream))
				{
					serializer.Serialize(bsonWriter, grainState.AsDictionary());

					SplitBinaryData(entity, memoryStream.ToArray());
				}
			}

			return _table.ExecuteAsync(TableOperation.InsertOrReplace(entity));
		}

		public Task ClearStateAsync(string grainType, GrainReference grainReference, GrainState grainState)
		{
			var entity = new DynamicTableEntity(grainReference.ToKeyString(), grainType) { ETag = "*" };

			return _table.ExecuteAsync(TableOperation.Delete(entity));
		}

		private void SplitBinaryData(DynamicTableEntity entity, byte[] data)
		{
			var dataSize = data.Length;
			int splitOffset = 0, splitCount = 0;
			while (splitOffset < dataSize)
			{
				int splitSize = (dataSize - splitOffset) > (64 * 1024) ? 64 * 1024 : dataSize - splitOffset;
				byte[] dataSplit = new byte[splitSize];
				Array.Copy(data, splitOffset, dataSplit, 0, splitSize);
				entity.Properties.Add(string.Format("d{0:X2}", splitCount), new EntityProperty(dataSplit));
				splitOffset += splitSize;
				splitCount++;
			}
		}

		private CloudTable _table;
	}
}
