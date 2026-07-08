using System;
using System.IO;
using System.Linq;
using Libplanet.Types.Blocks;
using Libplanet.RocksDBStore;
using Libplanet.Store;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace NineChronicles.Snapshot.Bridge
{
    class Program
    {
        const int EpochUnitSeconds = 86400;

        // Input: mesmo JSON que o Rust monta em fetch_metadata()
        public class BridgeInput
        {
            public string Apv { get; set; }
            public string OutputDirectory { get; set; } = ".";
            public string StorePath { get; set; }
            public int BlockBefore { get; set; } = 1;
            public bool BypassCopyStates { get; set; } = true;
            public string SnapshotType { get; set; } = "state";
        }

        // Output: casa 1:1 com o BridgeResult do types.rs (PascalCase)
        public class BridgeResult
        {
            public bool Success { get; set; }
            public string Error { get; set; }
            public string PartitionBaseFilename { get; set; } = "";
            public string StateBaseFilename { get; set; } = "state_latest";
            public int LatestEpoch { get; set; }
            public int CurrentMetadataBlockEpoch { get; set; }
            public int PreviousMetadataBlockEpoch { get; set; }
            public string StringfyMetadata { get; set; } = "";
        }

        static int Main(string[] args)
        {
            var result = new BridgeResult { Success = false };
            try
            {
                if (args.Length < 1)
                    throw new Exception("Expected a single JSON argument.");

                var input = JsonConvert.DeserializeObject<BridgeInput>(args[0])
                    ?? throw new Exception("Failed to parse input JSON.");

                if (string.IsNullOrEmpty(input.StorePath) || !Directory.Exists(input.StorePath))
                    throw new Exception($"Invalid store path: {input.StorePath}");

                RunMetadata(input, result);
                result.Success = true;
            }
            catch (Exception ex)
            {
                result.Success = false;
                result.Error = ex.Message;
            }

            // O Rust lê a ULTIMA linha do stdout como JSON. Logs vao pro stderr.
            Console.WriteLine(JsonConvert.SerializeObject(result));
            return result.Success ? 0 : 1;
        }

        static void RunMetadata(BridgeInput input, BridgeResult result)
        {
            var storePath = input.StorePath;
            var blockBefore = input.BlockBefore;
            var outputDirectory = string.IsNullOrEmpty(input.OutputDirectory) ? "." : input.OutputDirectory;

            // Epochs do metadata anterior (metadata/*.json mais recente)
            var metadataDirectory = Path.Combine(outputDirectory, "metadata");
            int currentMetadataBlockEpoch = GetMetaDataEpoch(metadataDirectory, "BlockEpoch");
            int currentMetadataTxEpoch    = GetMetaDataEpoch(metadataDirectory, "TxEpoch");
            int previousMetadataBlockEpoch = GetMetaDataEpoch(metadataDirectory, "PreviousBlockEpoch");

            // ── FIX: abre o store como SECONDARY (lock-safe com o no rodando) ──
            using var store = new RocksDBStore(storePath, type: RocksDBInstanceType.Secondary);

            var chainId = store.GetCanonicalChainId()
                ?? throw new Exception("Canonical chain doesn't exist.");

            var genesisHash = store.IterateIndexes(chainId, 0, 1).First();
            var tipHash = store.IndexBlockHash(chainId, -1)
                ?? throw new Exception("The given chain seems empty.");
            if (!(store.GetBlockIndex(tipHash) is { } tipIndex))
                throw new Exception($"The index of {tipHash} doesn't exist.");

            // states/ tambem em secondary, pro check .Recorded.
            // Se falhar (lock/versao), cai no fallback tipIndex - blockBefore.
            TrieStateStore stateStore = null;
            RocksDBKeyValueStore stateKv = null;
            try
            {
                var statesPath = Path.Combine(storePath, "states");
                // Se este overload nao existir na tua versao do Libplanet,
                // troca por: new RocksDBKeyValueStore(statesPath)
                stateKv = new RocksDBKeyValueStore(statesPath, RocksDBInstanceType.Secondary);
                stateStore = new TrieStateStore(stateKv);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(
                    $"[bridge] states secondary open falhou, usando blockBefore direto: {ex.Message}");
            }

            long snapshotTipIndex;
            BlockHash snapshotTipHash;

            if (stateStore != null)
            {
                // igual ao Program.cs oficial: anda pra frente ate um state root Recorded
                snapshotTipIndex = Math.Max(tipIndex - (blockBefore + 1), 0);
                do
                {
                    snapshotTipIndex++;
                    if (!(store.IndexBlockHash(chainId, snapshotTipIndex) is { } h))
                        throw new Exception($"Index {snapshotTipIndex} doesn't exist on {chainId}.");
                    snapshotTipHash = h;
                } while (!stateStore
                    .GetStateRoot(store.GetBlock(snapshotTipHash).StateRootHash)
                    .Recorded);
            }
            else
            {
                snapshotTipIndex = Math.Max(tipIndex - blockBefore, 0);
                snapshotTipHash = (BlockHash)store.IndexBlockHash(chainId, snapshotTipIndex)!;
            }

            var snapshotTip = store.GetBlock(snapshotTipHash);
            var snapshotTipDigest = store.GetBlockDigest(snapshotTipHash)
                ?? throw new Exception("Snapshot tip digest not found.");

            // latestEpoch sai do timestamp do snapshot tip (= newTip do fork oficial)
            int latestEpoch = (int)(snapshotTip.Timestamp.ToUnixTimeSeconds() / EpochUnitSeconds);

            var partitionBaseFilename = GetPartitionBaseFileName(
                currentMetadataBlockEpoch, currentMetadataTxEpoch, latestEpoch);

            var stringfyMetadata = CreateMetadata(
                snapshotTipDigest, input.Apv,
                currentMetadataBlockEpoch, currentMetadataTxEpoch,
                previousMetadataBlockEpoch, latestEpoch);

            stateStore?.Dispose();
            stateKv?.Dispose();

            result.PartitionBaseFilename    = partitionBaseFilename;
            result.StateBaseFilename        = "state_latest";
            result.LatestEpoch              = latestEpoch;
            result.CurrentMetadataBlockEpoch = currentMetadataBlockEpoch;
            result.PreviousMetadataBlockEpoch = previousMetadataBlockEpoch;
            result.StringfyMetadata         = stringfyMetadata;
        }

        // ─────────── helpers copiados do Program.cs oficial ───────────

        static string GetPartitionBaseFileName(
            int currentMetadataBlockEpoch, int currentMetadataTxEpoch, int latestEpoch)
        {
            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
                return $"snapshot-{latestEpoch - 1}-{latestEpoch - 1}";
            return $"snapshot-{latestEpoch}-{latestEpoch}";
        }

        static string CreateMetadata(
            BlockDigest snapshotTipDigest, string apv,
            int currentMetadataBlockEpoch, int currentMetadataTxEpoch,
            int previousMetadataBlockEpoch, int latestEpoch)
        {
            BlockHeader snapshotTipHeader = snapshotTipDigest.GetHeader();
            JObject jsonObject = JObject.FromObject(snapshotTipHeader);
            jsonObject.Add("APV", apv);
            jsonObject = AddPreviousEpochs(
                jsonObject, currentMetadataBlockEpoch, previousMetadataBlockEpoch,
                latestEpoch, "PreviousBlockEpoch", "PreviousTxEpoch");

            if (currentMetadataBlockEpoch == 0 && currentMetadataTxEpoch == 0)
            {
                jsonObject.Add("BlockEpoch", latestEpoch - 1);
                jsonObject.Add("TxEpoch", latestEpoch - 1);
            }
            else
            {
                jsonObject.Add("BlockEpoch", latestEpoch);
                jsonObject.Add("TxEpoch", latestEpoch);
            }

            // ToString(Formatting.None): mesma serializacao do bridge original,
            // garante JSON byte-compativel com o teu offline
            return jsonObject.ToString(Formatting.None);
        }

        static JObject AddPreviousEpochs(
            JObject jsonObject, int currentMetadataEpoch, int previousMetadataEpoch,
            int latestEpoch, string blockEpochName, string txEpochName)
        {
            if (currentMetadataEpoch == latestEpoch)
            {
                jsonObject.Add(blockEpochName, previousMetadataEpoch);
                jsonObject.Add(txEpochName, previousMetadataEpoch);
            }
            else
            {
                jsonObject.Add(blockEpochName, currentMetadataEpoch);
                jsonObject.Add(txEpochName, currentMetadataEpoch);
            }
            return jsonObject;
        }

        static int GetMetaDataEpoch(string outputDirectory, string epochType)
        {
            try
            {
                if (!Directory.Exists(outputDirectory)) return 0;
                string previousMetadata = Directory.GetFiles(outputDirectory)
                    .Where(x => Path.GetExtension(x) == ".json")
                    .OrderByDescending(File.GetLastWriteTime)
                    .First();
                var jsonObject = JObject.Parse(File.ReadAllText(previousMetadata));
                return (int)jsonObject[epochType];
            }
            catch (InvalidOperationException)
            {
                return 0;
            }
        }
    }
}
