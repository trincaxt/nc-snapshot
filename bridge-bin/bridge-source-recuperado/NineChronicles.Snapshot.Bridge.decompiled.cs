using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Libplanet.Common;
using Libplanet.Crypto;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Libplanet.Types.Blocks;
using Libplanet.Types.Tx;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

[assembly: CompilationRelaxations(8)]
[assembly: RuntimeCompatibility(WrapNonExceptionThrows = true)]
[assembly: Debuggable(DebuggableAttribute.DebuggingModes.IgnoreSymbolStoreSequencePoints)]
[assembly: TargetFramework(".NETCoreApp,Version=v8.0", FrameworkDisplayName = ".NET 8.0")]
[assembly: AssemblyCompany("NineChronicles.Snapshot.Bridge")]
[assembly: AssemblyConfiguration("Release")]
[assembly: AssemblyFileVersion("1.0.0.0")]
[assembly: AssemblyInformationalVersion("1.0.0+cf23b777e97f868248964725d93da8c43fe35765")]
[assembly: AssemblyProduct("NineChronicles.Snapshot.Bridge")]
[assembly: AssemblyTitle("NineChronicles.Snapshot.Bridge")]
[assembly: AssemblyVersion("1.0.0.0")]
[module: RefSafetyRules(11)]
namespace NineChronicles.Snapshot.Bridge;

public class PrepareArgs
{
	public string Apv { get; set; } = "";

	public string OutputDirectory { get; set; } = "";

	public string StorePath { get; set; } = "";

	public int BlockBefore { get; set; } = 1;

	public bool BypassCopyStates { get; set; }

	public string SnapshotType { get; set; } = "Partition";
}
public class PrepareResult
{
	public bool Success { get; set; }

	public string Error { get; set; } = "";

	public string PartitionBaseFilename { get; set; } = "";

	public string StateBaseFilename { get; set; } = "";

	public string FullSnapshotFilename { get; set; } = "";

	public int LatestEpoch { get; set; }

	public int CurrentMetadataBlockEpoch { get; set; }

	public int PreviousMetadataBlockEpoch { get; set; }

	public string StringfyMetadata { get; set; } = "";

	public double CopyStatesTimeMin { get; set; }
}
[JsonSourceGenerationOptions(WriteIndented = false, PropertyNamingPolicy = JsonKnownNamingPolicy.Unspecified)]
[JsonSerializable(typeof(PrepareArgs))]
[JsonSerializable(typeof(PrepareResult))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[GeneratedCode("System.Text.Json.SourceGeneration", "8.0.14.11203")]
internal class SourceGenerationContext : JsonSerializerContext, IJsonTypeInfoResolver
{
	private JsonTypeInfo<bool>? _Boolean;

	private JsonTypeInfo<double>? _Double;

	private JsonTypeInfo<PrepareArgs>? _PrepareArgs;

	private JsonTypeInfo<PrepareResult>? _PrepareResult;

	private JsonTypeInfo<Dictionary<string, string>>? _DictionaryStringString;

	private JsonTypeInfo<int>? _Int32;

	private JsonTypeInfo<string>? _String;

	private static readonly JsonSerializerOptions s_defaultOptions = new JsonSerializerOptions
	{
		PropertyNamingPolicy = null,
		WriteIndented = false
	};

	private static readonly JsonEncodedText PropName_Apv = JsonEncodedText.Encode("Apv");

	private static readonly JsonEncodedText PropName_OutputDirectory = JsonEncodedText.Encode("OutputDirectory");

	private static readonly JsonEncodedText PropName_StorePath = JsonEncodedText.Encode("StorePath");

	private static readonly JsonEncodedText PropName_BlockBefore = JsonEncodedText.Encode("BlockBefore");

	private static readonly JsonEncodedText PropName_BypassCopyStates = JsonEncodedText.Encode("BypassCopyStates");

	private static readonly JsonEncodedText PropName_SnapshotType = JsonEncodedText.Encode("SnapshotType");

	private static readonly JsonEncodedText PropName_Success = JsonEncodedText.Encode("Success");

	private static readonly JsonEncodedText PropName_Error = JsonEncodedText.Encode("Error");

	private static readonly JsonEncodedText PropName_PartitionBaseFilename = JsonEncodedText.Encode("PartitionBaseFilename");

	private static readonly JsonEncodedText PropName_StateBaseFilename = JsonEncodedText.Encode("StateBaseFilename");

	private static readonly JsonEncodedText PropName_FullSnapshotFilename = JsonEncodedText.Encode("FullSnapshotFilename");

	private static readonly JsonEncodedText PropName_LatestEpoch = JsonEncodedText.Encode("LatestEpoch");

	private static readonly JsonEncodedText PropName_CurrentMetadataBlockEpoch = JsonEncodedText.Encode("CurrentMetadataBlockEpoch");

	private static readonly JsonEncodedText PropName_PreviousMetadataBlockEpoch = JsonEncodedText.Encode("PreviousMetadataBlockEpoch");

	private static readonly JsonEncodedText PropName_StringfyMetadata = JsonEncodedText.Encode("StringfyMetadata");

	private static readonly JsonEncodedText PropName_CopyStatesTimeMin = JsonEncodedText.Encode("CopyStatesTimeMin");

	public JsonTypeInfo<bool> Boolean => _Boolean ?? (_Boolean = (JsonTypeInfo<bool>)base.Options.GetTypeInfo(typeof(bool)));

	public JsonTypeInfo<double> Double => _Double ?? (_Double = (JsonTypeInfo<double>)base.Options.GetTypeInfo(typeof(double)));

	public JsonTypeInfo<PrepareArgs> PrepareArgs => _PrepareArgs ?? (_PrepareArgs = (JsonTypeInfo<PrepareArgs>)base.Options.GetTypeInfo(typeof(PrepareArgs)));

	public JsonTypeInfo<PrepareResult> PrepareResult => _PrepareResult ?? (_PrepareResult = (JsonTypeInfo<PrepareResult>)base.Options.GetTypeInfo(typeof(PrepareResult)));

	public JsonTypeInfo<Dictionary<string, string>> DictionaryStringString => _DictionaryStringString ?? (_DictionaryStringString = (JsonTypeInfo<Dictionary<string, string>>)base.Options.GetTypeInfo(typeof(Dictionary<string, string>)));

	public JsonTypeInfo<int> Int32 => _Int32 ?? (_Int32 = (JsonTypeInfo<int>)base.Options.GetTypeInfo(typeof(int)));

	public JsonTypeInfo<string> String => _String ?? (_String = (JsonTypeInfo<string>)base.Options.GetTypeInfo(typeof(string)));

	public static SourceGenerationContext Default { get; } = new SourceGenerationContext(new JsonSerializerOptions(s_defaultOptions));

	protected override JsonSerializerOptions? GeneratedSerializerOptions { get; } = s_defaultOptions;

	private JsonTypeInfo<bool> Create_Boolean(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<bool> jsonTypeInfo))
		{
			jsonTypeInfo = JsonMetadataServices.CreateValueInfo<bool>(options, JsonMetadataServices.BooleanConverter);
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private JsonTypeInfo<double> Create_Double(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<double> jsonTypeInfo))
		{
			jsonTypeInfo = JsonMetadataServices.CreateValueInfo<double>(options, JsonMetadataServices.DoubleConverter);
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private JsonTypeInfo<PrepareArgs> Create_PrepareArgs(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<PrepareArgs> jsonTypeInfo))
		{
			JsonObjectInfoValues<PrepareArgs> objectInfo = new JsonObjectInfoValues<PrepareArgs>
			{
				ObjectCreator = () => new PrepareArgs(),
				ObjectWithParameterizedConstructorCreator = null,
				PropertyMetadataInitializer = (JsonSerializerContext _) => PrepareArgsPropInit(options),
				ConstructorParameterMetadataInitializer = null,
				SerializeHandler = PrepareArgsSerializeHandler
			};
			jsonTypeInfo = JsonMetadataServices.CreateObjectInfo(options, objectInfo);
			jsonTypeInfo.NumberHandling = null;
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private static JsonPropertyInfo[] PrepareArgsPropInit(JsonSerializerOptions options)
	{
		JsonPropertyInfo[] array = new JsonPropertyInfo[6];
		JsonPropertyInfoValues<string> propertyInfo = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).Apv,
			Setter = delegate(object obj, string? value)
			{
				((PrepareArgs)obj).Apv = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "Apv",
			JsonPropertyName = null
		};
		array[0] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo);
		JsonPropertyInfoValues<string> propertyInfo2 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).OutputDirectory,
			Setter = delegate(object obj, string? value)
			{
				((PrepareArgs)obj).OutputDirectory = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "OutputDirectory",
			JsonPropertyName = null
		};
		array[1] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo2);
		JsonPropertyInfoValues<string> propertyInfo3 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).StorePath,
			Setter = delegate(object obj, string? value)
			{
				((PrepareArgs)obj).StorePath = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "StorePath",
			JsonPropertyName = null
		};
		array[2] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo3);
		JsonPropertyInfoValues<int> propertyInfo4 = new JsonPropertyInfoValues<int>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).BlockBefore,
			Setter = delegate(object obj, int value)
			{
				((PrepareArgs)obj).BlockBefore = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "BlockBefore",
			JsonPropertyName = null
		};
		array[3] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo4);
		JsonPropertyInfoValues<bool> propertyInfo5 = new JsonPropertyInfoValues<bool>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).BypassCopyStates,
			Setter = delegate(object obj, bool value)
			{
				((PrepareArgs)obj).BypassCopyStates = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "BypassCopyStates",
			JsonPropertyName = null
		};
		array[4] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo5);
		JsonPropertyInfoValues<string> propertyInfo6 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareArgs),
			Converter = null,
			Getter = (object obj) => ((PrepareArgs)obj).SnapshotType,
			Setter = delegate(object obj, string? value)
			{
				((PrepareArgs)obj).SnapshotType = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "SnapshotType",
			JsonPropertyName = null
		};
		array[5] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo6);
		return array;
	}

	private void PrepareArgsSerializeHandler(Utf8JsonWriter writer, PrepareArgs? value)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}
		writer.WriteStartObject();
		writer.WriteString(PropName_Apv, value.Apv);
		writer.WriteString(PropName_OutputDirectory, value.OutputDirectory);
		writer.WriteString(PropName_StorePath, value.StorePath);
		writer.WriteNumber(PropName_BlockBefore, value.BlockBefore);
		writer.WriteBoolean(PropName_BypassCopyStates, value.BypassCopyStates);
		writer.WriteString(PropName_SnapshotType, value.SnapshotType);
		writer.WriteEndObject();
	}

	private JsonTypeInfo<PrepareResult> Create_PrepareResult(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<PrepareResult> jsonTypeInfo))
		{
			JsonObjectInfoValues<PrepareResult> objectInfo = new JsonObjectInfoValues<PrepareResult>
			{
				ObjectCreator = () => new PrepareResult(),
				ObjectWithParameterizedConstructorCreator = null,
				PropertyMetadataInitializer = (JsonSerializerContext _) => PrepareResultPropInit(options),
				ConstructorParameterMetadataInitializer = null,
				SerializeHandler = PrepareResultSerializeHandler
			};
			jsonTypeInfo = JsonMetadataServices.CreateObjectInfo(options, objectInfo);
			jsonTypeInfo.NumberHandling = null;
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private static JsonPropertyInfo[] PrepareResultPropInit(JsonSerializerOptions options)
	{
		JsonPropertyInfo[] array = new JsonPropertyInfo[10];
		JsonPropertyInfoValues<bool> propertyInfo = new JsonPropertyInfoValues<bool>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).Success,
			Setter = delegate(object obj, bool value)
			{
				((PrepareResult)obj).Success = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "Success",
			JsonPropertyName = null
		};
		array[0] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo);
		JsonPropertyInfoValues<string> propertyInfo2 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).Error,
			Setter = delegate(object obj, string? value)
			{
				((PrepareResult)obj).Error = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "Error",
			JsonPropertyName = null
		};
		array[1] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo2);
		JsonPropertyInfoValues<string> propertyInfo3 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).PartitionBaseFilename,
			Setter = delegate(object obj, string? value)
			{
				((PrepareResult)obj).PartitionBaseFilename = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "PartitionBaseFilename",
			JsonPropertyName = null
		};
		array[2] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo3);
		JsonPropertyInfoValues<string> propertyInfo4 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).StateBaseFilename,
			Setter = delegate(object obj, string? value)
			{
				((PrepareResult)obj).StateBaseFilename = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "StateBaseFilename",
			JsonPropertyName = null
		};
		array[3] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo4);
		JsonPropertyInfoValues<string> propertyInfo5 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).FullSnapshotFilename,
			Setter = delegate(object obj, string? value)
			{
				((PrepareResult)obj).FullSnapshotFilename = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "FullSnapshotFilename",
			JsonPropertyName = null
		};
		array[4] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo5);
		JsonPropertyInfoValues<int> propertyInfo6 = new JsonPropertyInfoValues<int>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).LatestEpoch,
			Setter = delegate(object obj, int value)
			{
				((PrepareResult)obj).LatestEpoch = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "LatestEpoch",
			JsonPropertyName = null
		};
		array[5] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo6);
		JsonPropertyInfoValues<int> propertyInfo7 = new JsonPropertyInfoValues<int>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).CurrentMetadataBlockEpoch,
			Setter = delegate(object obj, int value)
			{
				((PrepareResult)obj).CurrentMetadataBlockEpoch = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "CurrentMetadataBlockEpoch",
			JsonPropertyName = null
		};
		array[6] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo7);
		JsonPropertyInfoValues<int> propertyInfo8 = new JsonPropertyInfoValues<int>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).PreviousMetadataBlockEpoch,
			Setter = delegate(object obj, int value)
			{
				((PrepareResult)obj).PreviousMetadataBlockEpoch = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "PreviousMetadataBlockEpoch",
			JsonPropertyName = null
		};
		array[7] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo8);
		JsonPropertyInfoValues<string> propertyInfo9 = new JsonPropertyInfoValues<string>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).StringfyMetadata,
			Setter = delegate(object obj, string? value)
			{
				((PrepareResult)obj).StringfyMetadata = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "StringfyMetadata",
			JsonPropertyName = null
		};
		array[8] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo9);
		JsonPropertyInfoValues<double> propertyInfo10 = new JsonPropertyInfoValues<double>
		{
			IsProperty = true,
			IsPublic = true,
			IsVirtual = false,
			DeclaringType = typeof(PrepareResult),
			Converter = null,
			Getter = (object obj) => ((PrepareResult)obj).CopyStatesTimeMin,
			Setter = delegate(object obj, double value)
			{
				((PrepareResult)obj).CopyStatesTimeMin = value;
			},
			IgnoreCondition = null,
			HasJsonInclude = false,
			IsExtensionData = false,
			NumberHandling = null,
			PropertyName = "CopyStatesTimeMin",
			JsonPropertyName = null
		};
		array[9] = JsonMetadataServices.CreatePropertyInfo(options, propertyInfo10);
		return array;
	}

	private void PrepareResultSerializeHandler(Utf8JsonWriter writer, PrepareResult? value)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}
		writer.WriteStartObject();
		writer.WriteBoolean(PropName_Success, value.Success);
		writer.WriteString(PropName_Error, value.Error);
		writer.WriteString(PropName_PartitionBaseFilename, value.PartitionBaseFilename);
		writer.WriteString(PropName_StateBaseFilename, value.StateBaseFilename);
		writer.WriteString(PropName_FullSnapshotFilename, value.FullSnapshotFilename);
		writer.WriteNumber(PropName_LatestEpoch, value.LatestEpoch);
		writer.WriteNumber(PropName_CurrentMetadataBlockEpoch, value.CurrentMetadataBlockEpoch);
		writer.WriteNumber(PropName_PreviousMetadataBlockEpoch, value.PreviousMetadataBlockEpoch);
		writer.WriteString(PropName_StringfyMetadata, value.StringfyMetadata);
		writer.WriteNumber(PropName_CopyStatesTimeMin, value.CopyStatesTimeMin);
		writer.WriteEndObject();
	}

	private JsonTypeInfo<Dictionary<string, string>> Create_DictionaryStringString(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<Dictionary<string, string>> jsonTypeInfo))
		{
			JsonCollectionInfoValues<Dictionary<string, string>> collectionInfo = new JsonCollectionInfoValues<Dictionary<string, string>>
			{
				ObjectCreator = () => new Dictionary<string, string>(),
				SerializeHandler = DictionaryStringStringSerializeHandler
			};
			jsonTypeInfo = JsonMetadataServices.CreateDictionaryInfo<Dictionary<string, string>, string, string>(options, collectionInfo);
			jsonTypeInfo.NumberHandling = null;
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private void DictionaryStringStringSerializeHandler(Utf8JsonWriter writer, Dictionary<string, string>? value)
	{
		if (value == null)
		{
			writer.WriteNullValue();
			return;
		}
		writer.WriteStartObject();
		foreach (KeyValuePair<string, string> item in value)
		{
			writer.WriteString(item.Key, item.Value);
		}
		writer.WriteEndObject();
	}

	private JsonTypeInfo<int> Create_Int32(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<int> jsonTypeInfo))
		{
			jsonTypeInfo = JsonMetadataServices.CreateValueInfo<int>(options, JsonMetadataServices.Int32Converter);
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	private JsonTypeInfo<string> Create_String(JsonSerializerOptions options)
	{
		if (!TryGetTypeInfoForRuntimeCustomConverter(options, out JsonTypeInfo<string> jsonTypeInfo))
		{
			jsonTypeInfo = JsonMetadataServices.CreateValueInfo<string>(options, JsonMetadataServices.StringConverter);
		}
		jsonTypeInfo.OriginatingResolver = this;
		return jsonTypeInfo;
	}

	public SourceGenerationContext()
		: base(null)
	{
	}

	public SourceGenerationContext(JsonSerializerOptions options)
		: base(options)
	{
	}

	private static bool TryGetTypeInfoForRuntimeCustomConverter<TJsonMetadataType>(JsonSerializerOptions options, out JsonTypeInfo<TJsonMetadataType> jsonTypeInfo)
	{
		System.Text.Json.Serialization.JsonConverter runtimeConverterForType = GetRuntimeConverterForType(typeof(TJsonMetadataType), options);
		if (runtimeConverterForType != null)
		{
			jsonTypeInfo = JsonMetadataServices.CreateValueInfo<TJsonMetadataType>(options, runtimeConverterForType);
			return true;
		}
		jsonTypeInfo = null;
		return false;
	}

	private static System.Text.Json.Serialization.JsonConverter? GetRuntimeConverterForType(Type type, JsonSerializerOptions options)
	{
		for (int i = 0; i < options.Converters.Count; i++)
		{
			System.Text.Json.Serialization.JsonConverter jsonConverter = options.Converters[i];
			if (jsonConverter != null && jsonConverter.CanConvert(type))
			{
				return ExpandConverter(type, jsonConverter, options, validateCanConvert: false);
			}
		}
		return null;
	}

	private static System.Text.Json.Serialization.JsonConverter ExpandConverter(Type type, System.Text.Json.Serialization.JsonConverter converter, JsonSerializerOptions options, bool validateCanConvert = true)
	{
		if (validateCanConvert && !converter.CanConvert(type))
		{
			throw new InvalidOperationException($"The converter '{converter.GetType()}' is not compatible with the type '{type}'.");
		}
		if (converter is JsonConverterFactory jsonConverterFactory)
		{
			converter = jsonConverterFactory.CreateConverter(type, options);
			if (converter == null || converter is JsonConverterFactory)
			{
				throw new InvalidOperationException($"The converter '{jsonConverterFactory.GetType()}' cannot return null or a JsonConverterFactory instance.");
			}
		}
		return converter;
	}

	public override JsonTypeInfo? GetTypeInfo(Type type)
	{
		base.Options.TryGetTypeInfo(type, out JsonTypeInfo typeInfo);
		return typeInfo;
	}

	JsonTypeInfo? IJsonTypeInfoResolver.GetTypeInfo(Type type, JsonSerializerOptions options)
	{
		if (type == typeof(bool))
		{
			return Create_Boolean(options);
		}
		if (type == typeof(double))
		{
			return Create_Double(options);
		}
		if (type == typeof(PrepareArgs))
		{
			return Create_PrepareArgs(options);
		}
		if (type == typeof(PrepareResult))
		{
			return Create_PrepareResult(options);
		}
		if (type == typeof(Dictionary<string, string>))
		{
			return Create_DictionaryStringString(options);
		}
		if (type == typeof(int))
		{
			return Create_Int32(options);
		}
		if (type == typeof(string))
		{
			return Create_String(options);
		}
		return null;
	}
}
public static class Program
{
	public static void Main(string[] args)
	{
		try
		{
			if (args.Length != 0)
			{
				Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(ProcessSnapshot(System.Text.Json.JsonSerializer.Deserialize(args[0], SourceGenerationContext.Default.PrepareArgs) ?? new PrepareArgs()), SourceGenerationContext.Default.PrepareResult));
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(new PrepareResult
			{
				Success = false,
				Error = ex.ToString()
			}, SourceGenerationContext.Default.PrepareResult));
		}
	}

	private static PrepareResult ProcessSnapshot(PrepareArgs args)
	{
		PrepareResult prepareResult = new PrepareResult
		{
			Success = true
		};
		string outputDirectory = args.OutputDirectory;
		string storePath = args.StorePath;
		int num = args.BlockBefore;
		Console.Error.WriteLine("--> Loading metadata and calculating epochs...");
		string outputDirectory2 = Path.Combine(outputDirectory, "metadata");
		int metaDataEpoch = GetMetaDataEpoch(outputDirectory2, "BlockEpoch");
		int metaDataEpoch2 = GetMetaDataEpoch(outputDirectory2, "TxEpoch");
		int metaDataEpoch3 = GetMetaDataEpoch(outputDirectory2, "PreviousBlockEpoch");
		prepareResult.CurrentMetadataBlockEpoch = metaDataEpoch;
		prepareResult.PreviousMetadataBlockEpoch = metaDataEpoch3;
		string text = Path.Combine(storePath, "states");
		string text2 = Path.Combine(storePath, "9c-main");
		string text3 = Path.Combine(storePath, "stateref");
		string text4 = Path.Combine(storePath, "state");
		string text5 = Path.Combine(storePath, "new_states");
		string text6 = Path.Combine(storePath, "state_hashes");
		string[] array = new string[5] { text2, text4, text3, text6, text5 };
		foreach (string text7 in array)
		{
			if (Directory.Exists(text7))
			{
				Console.Error.WriteLine("--> Cleaning stale directory: " + text7);
				Directory.Delete(text7, recursive: true);
			}
		}
		Console.Error.WriteLine("--> Migrating RocksDB (if needed)...");
		RocksDBStore.MigrateChainDBFromColumnFamilies(Path.Combine(storePath, "chain"));
		Console.Error.WriteLine("--> Opening RocksDB Store...");
		using RocksDBStore rocksDBStore = new RocksDBStore(storePath);
		using RocksDBKeyValueStore rocksDBKeyValueStore = new RocksDBKeyValueStore(text);
		using TrieStateStore trieStateStore = new TrieStateStore(rocksDBKeyValueStore);
		Guid guid = rocksDBStore.GetCanonicalChainId() ?? throw new Exception("Canonical chain doesn't exist.");
		BlockHash blockHash = rocksDBStore.IterateIndexes(guid, 0, 1).First();
		BlockHash blockHash2 = rocksDBStore.IndexBlockHash(guid, -1L) ?? throw new Exception("Empty chain.");
		long num2 = rocksDBStore.GetBlockIndex(blockHash2) ?? throw new Exception($"Index of {blockHash2} doesn't exist.");
		Block block = rocksDBStore.GetBlock(blockHash2);
		long index = num2 - num;
		BlockHash value = rocksDBStore.IndexBlockHash(guid, index).Value;
		rocksDBStore.GetBlock(value);
		BlockCommit blockCommit = rocksDBStore.GetBlockCommit(blockHash2) ?? GetChainBlockCommit(rocksDBStore, blockHash2, guid);
		BlockCommit blockCommit2 = rocksDBStore.GetBlockCommit(value) ?? GetChainBlockCommit(rocksDBStore, value, guid);
		if (blockCommit2 != null)
		{
			rocksDBStore.PutBlockCommit(blockCommit);
			rocksDBStore.PutChainBlockCommit(guid, blockCommit);
			rocksDBStore.PutBlockCommit(blockCommit2);
			rocksDBStore.PutChainBlockCommit(guid, blockCommit2);
		}
		else
		{
			num++;
			blockCommit2 = rocksDBStore.GetBlock(rocksDBStore.IndexBlockHash(guid, block.Index - num + 1).Value).LastCommit;
			rocksDBStore.PutBlockCommit(blockCommit);
			rocksDBStore.PutChainBlockCommit(guid, blockCommit);
			rocksDBStore.PutBlockCommit(blockCommit2);
			rocksDBStore.PutChainBlockCommit(guid, blockCommit2);
		}
		Block block2 = rocksDBStore.GetBlock(blockHash2);
		for (int j = 0; j < num + 5; j++)
		{
			rocksDBStore.PutBlockCommit(block2.LastCommit);
			rocksDBStore.PutChainBlockCommit(guid, block2.LastCommit);
			block2 = rocksDBStore.GetBlock(block2.PreviousHash.Value);
		}
		long num3 = Math.Max(num2 - (num + 1), 0L);
		BlockHash blockHash4;
		do
		{
			num3++;
			BlockHash blockHash3 = rocksDBStore.IndexBlockHash(guid, num3) ?? throw new Exception($"Index {num3} doesn't exist.");
			blockHash4 = blockHash3;
		}
		while (!trieStateStore.GetStateRoot(rocksDBStore.GetBlock(blockHash4).StateRootHash).Recorded);
		Guid forkedId = Guid.NewGuid();
		Fork(rocksDBStore, guid, forkedId, blockHash4, block);
		rocksDBStore.SetCanonicalChainId(forkedId);
		foreach (Guid item in from id in rocksDBStore.ListChainIds()
			where !id.Equals(forkedId)
			select id)
		{
			rocksDBStore.DeleteChainId(item);
		}
		BlockDigest? blockDigest = rocksDBStore.GetBlockDigest(blockHash4);
		HashDigest<SHA256>? stateRootHash = rocksDBStore.GetStateRootHash(blockHash4);
		ImmutableHashSet<HashDigest<SHA256>> immutableHashSet = ImmutableHashSet<HashDigest<SHA256>>.Empty.Add(stateRootHash.Value);
		BlockHash? blockHash5 = blockDigest?.Hash;
		int num4 = 0;
		while (blockHash5.HasValue)
		{
			BlockHash valueOrDefault = blockHash5.GetValueOrDefault();
			BlockDigest? blockDigest2 = rocksDBStore.GetBlockDigest(valueOrDefault);
			if (!blockDigest2.HasValue)
			{
				break;
			}
			BlockDigest valueOrDefault2 = blockDigest2.GetValueOrDefault();
			if (num4 >= 2)
			{
				break;
			}
			immutableHashSet = immutableHashSet.Add(valueOrDefault2.StateRootHash);
			blockHash5 = valueOrDefault2.PreviousHash;
			num4++;
		}
		BlockHash blockHash6 = rocksDBStore.IndexBlockHash(forkedId, -1L) ?? throw new Exception("Empty chain after fork.");
		int latestEpoch = (prepareResult.LatestEpoch = (int)(rocksDBStore.GetBlock(blockHash6).Timestamp.ToUnixTimeSeconds() / 86400));
		if (!args.BypassCopyStates)
		{
			using RocksDBKeyValueStore stateKeyValueStore = new RocksDBKeyValueStore(text5);
			using TrieStateStore targetStateStore = new TrieStateStore(stateKeyValueStore);
			Console.Error.WriteLine("--> Starting CopyStates (this may take a few minutes)...");
			DateTimeOffset now = DateTimeOffset.Now;
			trieStateStore.CopyStates(immutableHashSet, targetStateStore);
			prepareResult.CopyStatesTimeMin = (DateTimeOffset.Now - now).TotalMinutes;
			Console.Error.WriteLine($"--> CopyStates finished in {prepareResult.CopyStatesTimeMin:F2} minutes.");
		}
		Console.Error.WriteLine("--> Finalizing C# processing...");
		rocksDBStore.Dispose();
		trieStateStore.Dispose();
		rocksDBKeyValueStore.Dispose();
		if (Directory.Exists(text5))
		{
			Console.Error.WriteLine("--> Moving new states to permanent location...");
			Directory.Delete(text, recursive: true);
			Directory.Move(text5, text);
		}
		string partitionBaseFileName = GetPartitionBaseFileName(metaDataEpoch, metaDataEpoch2, latestEpoch);
		prepareResult.PartitionBaseFilename = partitionBaseFileName;
		prepareResult.StateBaseFilename = "state_latest";
		string value2 = ByteUtil.Hex(blockHash.ToByteArray());
		string value3 = ByteUtil.Hex(blockHash4.ToByteArray());
		prepareResult.FullSnapshotFilename = $"{value2}-snapshot-{value3}-{num3}";
		if (blockDigest.HasValue)
		{
			prepareResult.StringfyMetadata = CreateMetadata(blockDigest.Value, args.Apv, metaDataEpoch, metaDataEpoch2, metaDataEpoch3, latestEpoch);
		}
		return prepareResult;
	}

	private static void Fork(RocksDBStore store, Guid src, Guid dest, BlockHash branchpointHash, Block tip)
	{
		store.ForkBlockIndexes(src, dest, branchpointHash);
		if (store.GetBlockCommit(branchpointHash) != null)
		{
			store.PutChainBlockCommit(dest, store.GetBlockCommit(branchpointHash));
		}
		store.ForkTxNonces(src, dest);
		Block block = tip;
		while (true)
		{
			BlockHash? previousHash = block.PreviousHash;
			if (!previousHash.HasValue)
			{
				break;
			}
			BlockHash valueOrDefault = previousHash.GetValueOrDefault();
			if (block.Hash.Equals(branchpointHash))
			{
				break;
			}
			foreach (var (signer, num) in from tx in block.Transactions
				group tx by tx.Signer into g
				select (Key: g.Key, g.Count()))
			{
				store.IncreaseTxNonce(dest, signer, -num);
			}
			block = store.GetBlock(valueOrDefault);
		}
	}

	private static int GetMetaDataEpoch(string outputDirectory, string epochType)
	{
		try
		{
			if (!Directory.Exists(outputDirectory))
			{
				return 0;
			}
			List<string> source = (from x in Directory.GetFiles(outputDirectory)
				where Path.GetExtension(x) == ".json"
				select x).ToList();
			if (!source.Any())
			{
				return 0;
			}
			if (JsonDocument.Parse(File.ReadAllText(source.OrderByDescending((string x) => File.GetLastWriteTime(x)).First())).RootElement.TryGetProperty(epochType, out var value))
			{
				return value.GetInt32();
			}
			return 0;
		}
		catch
		{
			return 0;
		}
	}

	private static BlockCommit GetChainBlockCommit(RocksDBStore store, BlockHash blockHash, Guid chainId)
	{
		BlockHash blockHash2 = store.IndexBlockHash(chainId, -1L) ?? throw new Exception("Empty chain.");
		long num = store.GetBlockIndex(blockHash2) ?? throw new Exception("Tip index missing.");
		long num2 = store.GetBlockIndex(blockHash) ?? throw new Exception("Block index missing.");
		if (num2 == num)
		{
			return store.GetChainBlockCommit(chainId);
		}
		BlockHash blockHash3 = store.IndexBlockHash(chainId, num2 + 1) ?? throw new Exception("Next hash missing.");
		return store.GetBlock(blockHash3).LastCommit;
	}

	private static string GetPartitionBaseFileName(int currentMetadataBlockEpoch, int currentMetadataTxEpoch, int latestEpoch)
	{
		if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
		{
			return $"snapshot-{latestEpoch - 1}-{latestEpoch - 1}";
		}
		return $"snapshot-{latestEpoch}-{latestEpoch}";
	}

	private static string CreateMetadata(BlockDigest snapshotTipDigest, string apv, int currentMetadataBlockEpoch, int currentMetadataTxEpoch, int previousMetadataBlockEpoch, int latestEpoch)
	{
		JObject jObject = JObject.FromObject(snapshotTipDigest.GetHeader());
		jObject.Add("APV", apv);
		jObject.Add("PreviousBlockEpoch", (currentMetadataBlockEpoch == latestEpoch) ? previousMetadataBlockEpoch : currentMetadataBlockEpoch);
		jObject.Add("PreviousTxEpoch", (currentMetadataBlockEpoch == latestEpoch) ? previousMetadataBlockEpoch : currentMetadataBlockEpoch);
		if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
		{
			jObject.Add("BlockEpoch", latestEpoch - 1);
			jObject.Add("TxEpoch", latestEpoch - 1);
		}
		else
		{
			jObject.Add("BlockEpoch", latestEpoch);
			jObject.Add("TxEpoch", latestEpoch);
		}
		return jObject.ToString(Formatting.None);
	}
}
