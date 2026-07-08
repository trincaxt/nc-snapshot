using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Libplanet.RocksDBStore;
using Newtonsoft.Json;

namespace CheckpointBridge
{
    // ----------------------------------------------------------------
    // P/Invoke para a API C nativa do RocksDB
    // ----------------------------------------------------------------
    internal static class RocksDbNative
    {
        private const string Lib = "rocksdb";

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr rocksdb_options_create();

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_options_destroy(IntPtr options);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(
            IntPtr options, byte val);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_options_set_paranoid_checks(
            IntPtr options, byte val);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        public static extern IntPtr rocksdb_open_as_secondary(
            IntPtr options,
            string db_path,
            string secondary_path,
            out IntPtr errptr);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_try_catch_up_with_primary(
            IntPtr db,
            out IntPtr errptr);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr rocksdb_checkpoint_object_create(
            IntPtr db,
            out IntPtr errptr);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        public static extern void rocksdb_checkpoint_create(
            IntPtr checkpoint,
            string checkpoint_dir,
            ulong log_size_for_flush,
            out IntPtr errptr);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_checkpoint_object_destroy(IntPtr checkpoint);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_close(IntPtr db);

        [DllImport(Lib, CallingConvention = CallingConvention.Cdecl)]
        public static extern void rocksdb_free(IntPtr ptr);

        public static string ReadAndFreeError(ref IntPtr errptr)
        {
            if (errptr == IntPtr.Zero)
                return null;

            string msg = Marshal.PtrToStringAnsi(errptr);
            rocksdb_free(errptr);
            errptr = IntPtr.Zero;
            return msg;
        }
    }
    public class StateRootResult
    {
        public bool Success { get; set; }
        public string StateRootHash { get; set; }
        public long BlockIndex { get; set; }
        public string BlockHash { get; set; }
        public string Error { get; set; }
    }

    public class CheckpointResult
    {
        public bool Success { get; set; }
        public string ValidatedPath { get; set; }
        public string Error { get; set; }
    }

    // Helper para Hash32 (usado no GC Pipeline)
    public struct Hash32 : IEquatable<Hash32>
    {
        private readonly byte[] _bytes;

        public Hash32(byte[] bytes)
        {
            if (bytes.Length != 32)
                throw new ArgumentException("Hash32 must be 32 bytes");
            _bytes = bytes;
        }

        public byte[] ToBytes() => (byte[])_bytes.Clone();

        public override bool Equals(object obj) => obj is Hash32 other && Equals(other);
        public bool Equals(Hash32 other) => _bytes.SequenceEqual(other._bytes);
        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                for (int i = 0; i < 32; i++)
                    hash = hash * 31 + _bytes[i];
                return hash;
            }
        }

        public static bool operator ==(Hash32 a, Hash32 b) => a.Equals(b);
        public static bool operator !=(Hash32 a, Hash32 b) => !a.Equals(b);
    }

    // Implementação mínima do BFS para GC Pipeline
    public static class GcBfsFilter
    {
        private const int HASH_SIZE = 32;
        private const int VALUE_HASH_SIZE = 32;

        public static byte[] ParseHex(string hex)
        {
            if (hex.Length != 64)
                throw new ArgumentException("Invalid hex string (must be 64 chars)");
            byte[] bytes = new byte[32];
            for (int i = 0; i < 32; i++)
                bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
            return bytes;
        }

        public static HashSet<Hash32> RunBfs(string exportFile, byte[][] roots)
        {
            var visited = new HashSet<Hash32>();
            var queue = new Queue<Hash32>();
            var keyToValue = new Dictionary<Hash32, byte[]>();

            // Carrega exportação
            using (var fs = File.OpenRead(exportFile))
            using (var reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    byte[] key = reader.ReadBytes(HASH_SIZE);
                    int valLen = reader.ReadInt32();
                    byte[] val = reader.ReadBytes(valLen);
                    keyToValue[new Hash32(key)] = val;
                }
            }

            // Inicializa BFS
            foreach (var root in roots)
            {
                var h = new Hash32(root);
                if (keyToValue.ContainsKey(h))
                {
                    visited.Add(h);
                    queue.Enqueue(h);
                }
            }

            // BFS
            while (queue.Count > 0)
            {
                var current = queue.Dequeue();
                if (!keyToValue.TryGetValue(current, out var value))
                    continue;

                // Deserializa o valor (formato: 32 bytes de hash do estado + 32 bytes do endereço do nó)
                // Os primeiros 32 bytes são o hash do estado, os próximos 32 são o address
                // Como no BFS original, caminhamos pelos hashes dos estados
                if (value.Length >= HASH_SIZE)
                {
                    for (int offset = 0; offset + HASH_SIZE <= value.Length; offset += HASH_SIZE)
                    {
                        var childHash = new Hash32(value.Skip(offset).Take(HASH_SIZE).ToArray());
                        if (keyToValue.ContainsKey(childHash) && !visited.Contains(childHash))
                        {
                            visited.Add(childHash);
                            queue.Enqueue(childHash);
                        }
                    }
                }
            }

            return visited;
        }
    }

    class Program
    {
        static int Main(string[] args)
        {
// ════════════════════════════════════════════════════════════════
            // GC VALIDATE ONLY (NOVO!)
            // ════════════════════════════════════════════════════════════════
            if (args.Length >= 2 && args[0] == "--gc-validate")
                return GcValidate(args);

            // ════════════════════════════════════════════════════════════════
            // GC EXPORT ONLY (NOVO!)
            // ════════════════════════════════════════════════════════════════
            if (args.Length >= 3 && args[0] == "--gc-export")
                return GcExport(args);

            // ════════════════════════════════════════════════════════════════
            // GC PRUNE ONLY (NOVO!)
            // ════════════════════════════════════════════════════════════════
            if (args.Length >= 4 && args[0] == "--gc-prune")
                return GcPrune(args);

            // ════════════════════════════════════════════════════════════
            // GC PIPELINE COMMAND
            // ════════════════════════════════════════════════════════════
            if (args.Length >= 4 && args[0] == "--gc-pipeline")
                return GcPipeline(args);

            // ════════════════════════════════════════════════════════════
            // GET STATE ROOT COMMAND
            // ════════════════════════════════════════════════════════════
            if (args.Length >= 2 && args[0] == "--get-state-root")
                return GetStateRoot(args);

            if (args.Length < 2)
            {
                var usage = new CheckpointResult
                {
                    Success = false,
                    Error = "Usage: CheckpointBridge [--batch-epochs <epoch-limit>] <source-db-path> <destination-checkpoint-path>"
                };
                Console.WriteLine(JsonConvert.SerializeObject(usage));
                return 1;
            }

            bool batchMode = false;
            int epochLimit = 0;
            string sourceDb;
            string destCheckpoint;

            // Parse arguments
            if (args[0] == "--batch-epochs" && args.Length >= 4)
            {
                batchMode = true;
                if (!int.TryParse(args[1], out epochLimit))
                {
                    var error = new CheckpointResult
                    {
                        Success = false,
                        Error = $"Invalid epoch limit: {args[1]}"
                    };
                    Console.WriteLine(JsonConvert.SerializeObject(error));
                    return 1;
                }
                sourceDb = args[2];
                destCheckpoint = args[3];
            }
            else
            {
                sourceDb = args[0];
                destCheckpoint = args[1];
            }

            try
            {
                // Validar que source existe
                if (!Directory.Exists(sourceDb))
                {
                    var result = new CheckpointResult
                    {
                        Success = false,
                        Error = $"Source database not found: {sourceDb}"
                    };
                    Console.WriteLine(JsonConvert.SerializeObject(result));
                    return 1;
                }

                if (batchMode)
                {
                    // Batch mode: process all epoch* subdirectories
                    var epochDirs = Directory.GetDirectories(sourceDb, "epoch*")
                    .Select(d => new { Path = d, Name = Path.GetFileName(d) })
                    .Where(e => {
                        var match = System.Text.RegularExpressions.Regex.Match(e.Name, @"^epoch(\d+)$");
                        if (match.Success && int.TryParse(match.Groups[1].Value, out int epoch))
                        {
                            return epoch >= epochLimit;
                        }
                        return false;
                    })
                    .OrderBy(e => e.Name)
                    .ToList();

                    Console.Error.WriteLine($"Processing {epochDirs.Count} epochs (>= {epochLimit})...");

                    int processed = 0;
                    foreach (var epochDir in epochDirs)
                    {
                        processed++;
                        var destPath = Path.Combine(destCheckpoint, epochDir.Name);
                        Console.Error.WriteLine($"[{processed}/{epochDirs.Count}] {epochDir.Name}");
                        CheckpointSingleRocksDb(epochDir.Path, destPath);
                    }

                    var success = new CheckpointResult
                    {
                        Success = true,
                        ValidatedPath = destCheckpoint,
                        Error = null
                    };
                    Console.WriteLine(JsonConvert.SerializeObject(success));
                    return 0;
                }
                else
                {
                    // Single mode: process one DB
                    string validatedPath = CheckpointSingleRocksDb(sourceDb, destCheckpoint);

                    var success = new CheckpointResult
                    {
                        Success = true,
                        ValidatedPath = validatedPath,
                        Error = null
                    };
                    Console.WriteLine(JsonConvert.SerializeObject(success));
                    return 0;
                }
            }
            catch (Exception ex)
            {
                var error = new CheckpointResult
                {
                    Success = false,
                    Error = ex.Message
                };
                Console.WriteLine(JsonConvert.SerializeObject(error));
                return 1;
            }
        }

/// <summary>
        /// Valida rapidamente um states/ com Libplanet
        /// Uso: CheckpointBridge --gc-validate <states-path>
        /// </summary>
        static int GcValidate(string[] args)
        {
            string statesPath = args[1];

            try
            {
                Console.Error.WriteLine("[GC] Validating pruned states/...");
                Console.Error.Flush();

                using var store = new RocksDBStore(statesPath);
                var chainId = store.GetCanonicalChainId();

                Console.Error.WriteLine("[GC] ✅ Validation complete: states/ is valid!");
                Console.Error.Flush();

                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = true,
                    Message = "states/ validated successfully"
                }));
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = ex.Message
                }));
                return 1;
            }
        }

        /// <summary>
        /// Obtém o StateRoot Hash e BlockIndex de um checkpoint.
        /// Uso: CheckpointBridge --get-state-root <checkpoint-path> --block-before <N>
        /// </summary>
        /// <summary>
        /// Lê o StateRootHash do blockchain usando Libplanet
        /// </summary>
        static int GetStateRoot(string[] args)
        {
            try
            {
                string storePath = args[1];
                int blockBefore = 0;

                // Parse optional --block-before
                if (args.Length >= 4 && args[2] == "--block-before")
                {
                    if (!int.TryParse(args[3], out blockBefore))
                    {
                        var error = new StateRootResult
                        {
                            Success = false,
                            Error = $"Invalid block-before value: {args[3]}"
                        };
                        Console.WriteLine(JsonConvert.SerializeObject(error));
                        return 1;
                    }
                }

                if (!Directory.Exists(storePath))
                {
                    var error = new StateRootResult
                    {
                        Success = false,
                        Error = $"Store path not found: {storePath}"
                    };
                    Console.WriteLine(JsonConvert.SerializeObject(error));
                    return 1;
                }

                // Abrir o store com Libplanet em modo Secondary
                using (var store = new RocksDBStore(storePath, type: RocksDBInstanceType.Secondary))
                {
                    var chainId = store.GetCanonicalChainId();

                    if (chainId == null)
                    {
                        var error = new StateRootResult
                        {
                            Success = false,
                            Error = "No canonical chain ID found in store"
                        };
                        Console.WriteLine(JsonConvert.SerializeObject(error));
                        return 1;
                    }

                    // Pegar o tip block index usando CountIndex
                    long tipIndex = store.CountIndex(chainId.Value) - 1;

                    if (tipIndex < 0)
                    {
                        var error = new StateRootResult
                        {
                            Success = false,
                            Error = "No blocks found in chain"
                        };
                        Console.WriteLine(JsonConvert.SerializeObject(error));
                        return 1;
                    }

                    // Aplicar block-before
                    long targetIndex = Math.Max(0, tipIndex - blockBefore);

                    // Pegar block hash do índice
                    var blockHash = store.IndexBlockHash(chainId.Value, targetIndex);

                    if (blockHash == null)
                    {
                        var error = new StateRootResult
                        {
                            Success = false,
                            Error = $"Block not found at index {targetIndex}"
                        };
                        Console.WriteLine(JsonConvert.SerializeObject(error));
                        return 1;
                    }

                    // Pegar o block para ler o StateRootHash
                    var block = store.GetBlock(blockHash.Value);

                    if (block == null)
                    {
                        var error = new StateRootResult
                        {
                            Success = false,
                            Error = $"Block data not found for hash {blockHash.Value}"
                        };
                        Console.WriteLine(JsonConvert.SerializeObject(error));
                        return 1;
                    }

                    // Retornar o StateRootHash
                    var result = new StateRootResult
                    {
                        Success = true,
                        StateRootHash = block.StateRootHash.ToString(),
                        BlockIndex = targetIndex,
                        BlockHash = blockHash.Value.ToString(),
                        Error = null
                    };

                    Console.WriteLine(JsonConvert.SerializeObject(result));
                    return 0;
                }
            }
            catch (Exception ex)
            {
                var error = new StateRootResult
                {
                    Success = false,
                    Error = ex.Message
                };
                Console.WriteLine(JsonConvert.SerializeObject(error));
                return 1;
            }
        }

        /// <summary>
        /// Exporta apenas: states/ → states_export.bin
        /// Uso: CheckpointBridge --gc-export <source-states> <export-file>
        /// </summary>
        static int GcExport(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = "Usage: --gc-export <source-states> <export-file>"
                }));
                return 1;
            }

            string sourcePath = args[1];
            string exportFile = args[2];

            try
            {
                Console.Error.WriteLine("[GC] Phase 1: Exporting states/...");
                Console.Error.Flush();

                long totalEntries = 0;
                var srcOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(false);

                using (var srcDb = RocksDbSharp.RocksDb.OpenReadOnly(srcOptions, sourcePath, false))
                using (var outStream = File.Create(exportFile))
                using (var writer = new BinaryWriter(outStream))
                {
                    using var iter = srcDb.NewIterator();
                    for (iter.SeekToFirst(); iter.Valid(); iter.Next())
                    {
                        var key = iter.Key();
                        var val = iter.Value();
                        if (key.Length != 32) continue;
                        writer.Write(key);
                        writer.Write(val.Length);
                        writer.Write(val);
                        totalEntries++;
                        if (totalEntries % 10_000_000 == 0)
                        {
                            Console.Error.WriteLine($"[GC] Exported {totalEntries / 1_000_000}M entries...");
                            Console.Error.Flush();
                        }
                    }
                }

                Console.Error.WriteLine($"[GC] Export complete: {totalEntries:N0} entries");
                Console.Error.Flush();

                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = true,
                    TotalEntries = totalEntries,
                    ExportFile = exportFile
                }));
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = ex.Message
                }));
                return 1;
            }
        }

        /// <summary>
        /// Prune apenas: usa live_keys.bin para limpar states/
        /// Uso: CheckpointBridge --gc-prune <source-states> <dest-states> <live-keys-file>
        /// </summary>
        static int GcPrune(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = "Usage: --gc-prune <source-states> <dest-states> <live-keys-file>"
                }));
                return 1;
            }

            string sourcePath = args[1];
            string destPath = args[2];
            string liveKeysFile = args[3];

            try
            {
                Console.Error.WriteLine("[GC] Phase 3: Writing pruned states...");
                Console.Error.Flush();

                // Carregar live keys
                var liveKeys = new HashSet<Hash32>();
                using (var fs = File.OpenRead(liveKeysFile))
                using (var reader = new BinaryReader(fs))
                {
                    while (fs.Position < fs.Length)
                    {
                        var key = reader.ReadBytes(32);
                        liveKeys.Add(new Hash32(key));
                    }
                }

                Console.Error.WriteLine($"[GC] Loaded {liveKeys.Count:N0} live keys");
                Console.Error.Flush();

                // Copiar DB
                if (Directory.Exists(destPath)) Directory.Delete(destPath, true);
                CopyDirectory(sourcePath, destPath);

                var tableOptions = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(5);
                var dbOptions = new RocksDbSharp.DbOptions()
                .SetCreateIfMissing(false)
                .SetBlockBasedTableFactory(tableOptions);

                long deleted = 0;
                using (var destDb = RocksDbSharp.RocksDb.Open(dbOptions, destPath))
                {
                    using var batch = new RocksDbSharp.WriteBatch();
                    int batchCount = 0;
                    using var iter = destDb.NewIterator();

                    for (iter.SeekToFirst(); iter.Valid(); iter.Next())
                    {
                        var key = iter.Key();
                        if (key.Length == 32 && !liveKeys.Contains(new Hash32(key)))
                        {
                            batch.Delete(key);
                            deleted++;
                            batchCount++;
                            if (batchCount >= 50_000)
                            {
                                destDb.Write(batch);
                                batch.Clear();
                                batchCount = 0;
                                if (deleted % 1_000_000 == 0)
                                {
                                    Console.Error.WriteLine($"[GC] Deleted {deleted / 1_000_000}M garbage...");
                                    Console.Error.Flush();
                                }
                            }
                        }
                    }
                    if (batchCount > 0) destDb.Write(batch);

                    Console.Error.WriteLine("[GC] Compacting...");
                    Console.Error.Flush();
                    destDb.CompactRange(new byte[0], new byte[] { 0xFF });
                }

                Console.Error.WriteLine($"[GC] Prune complete: {deleted:N0} deleted");
                Console.Error.Flush();

                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = true,
                    DeletedNodes = deleted
                }));
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = ex.Message
                }));
                return 1;
            }
        }


        /// <summary>
        /// Executa o GC Pipeline completo nas 3 fases.
        /// Uso: CheckpointBridge --gc-pipeline <source-states> <dest-states> <root-hash-hex>
        /// Retorna JSON: { "Success": true, "NodesCopied": N, "ElapsedMinutes": X, "DestSizeGiB": Y }
        /// </summary>
        static int GcPipeline(string[] args)
        {
            if (args.Length < 4)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error = "Usage: --gc-pipeline <source-states> <dest-states> <root-hash-hex>"
                }));
                return 1;
            }

            string sourcePath = args[1];
            string destPath   = args[2];
            string rootHex    = args[3];

            try
            {
                if (!Directory.Exists(sourcePath))
                {
                    Console.WriteLine(JsonConvert.SerializeObject(new {
                        Success = false,
                        Error = $"Source states/ not found: {sourcePath}"
                    }));
                    return 1;
                }

                if (rootHex.Length != 64)
                {
                    Console.WriteLine(JsonConvert.SerializeObject(new {
                        Success = false,
                        Error = $"Invalid root hash (expected 64 hex chars): {rootHex}"
                    }));
                    return 1;
                }

                var sw = System.Diagnostics.Stopwatch.StartNew();

                // ── ARQUIVOS DIRETO NA RAIZ DO CHECKPOINT (sem tempDir) ──
                var checkpointDir = Path.GetDirectoryName(sourcePath)!;
                var exportFile = Path.Combine(checkpointDir, "states_export.bin");
                var liveKeysFile = Path.Combine(checkpointDir, "live_keys.bin");

                // ── Phase 1: Export ─────────────────────────────────────────────
                Console.Error.WriteLine("[GC] Phase 1: Exporting states/...");
                Console.Error.Flush();

                long totalEntries = 0;

                var srcOptions = new RocksDbSharp.DbOptions().SetCreateIfMissing(false);
                using (var srcDb = RocksDbSharp.RocksDb.OpenReadOnly(srcOptions, sourcePath, false))
                using (var outStream = File.Create(exportFile))
                using (var writer = new System.IO.BinaryWriter(outStream))
                {
                    using var iter = srcDb.NewIterator();
                    for (iter.SeekToFirst(); iter.Valid(); iter.Next())
                    {
                        var key = iter.Key();
                        var val = iter.Value();
                        if (key.Length != 32) continue;
                        writer.Write(key);
                        writer.Write(val.Length);
                        writer.Write(val);
                        totalEntries++;
                        if (totalEntries % 10_000_000 == 0)
                        {
                            Console.Error.WriteLine($"[GC] Exported {totalEntries / 1_000_000}M entries...");
                            Console.Error.Flush();
                        }
                    }
                }
                Console.Error.WriteLine($"[GC] Phase 1 done: {totalEntries:N0} entries | {sw.Elapsed.TotalMinutes:F1}min");
                Console.Error.Flush();

                // ── Phase 2: BFS ─────────────────────────────────────────────────
                Console.Error.WriteLine("[GC] Phase 2: BFS...");
                Console.Error.Flush();

                var rootBytes = GcBfsFilter.ParseHex(rootHex);
                var roots = new[] { rootBytes };
                var liveKeys = GcBfsFilter.RunBfs(exportFile, roots);

                Console.Error.WriteLine($"[GC] Phase 2 done: {liveKeys.Count:N0} live nodes | {sw.Elapsed.TotalMinutes:F1}min");
                Console.Error.Flush();

                // Escreve live keys
                using (var lkStream = File.Create(liveKeysFile))
                using (var lkWriter = new System.IO.BinaryWriter(lkStream))
                foreach (var k in liveKeys) lkWriter.Write(k.ToBytes());

                // ── Phase 3: Write pruned states ────────────────────────────────
                Console.Error.WriteLine("[GC] Phase 3: Writing pruned states...");
                Console.Error.Flush();

                if (Directory.Exists(destPath)) Directory.Delete(destPath, true);
                CopyDirectory(sourcePath, destPath);

                var tableOptions = new RocksDbSharp.BlockBasedTableOptions().SetFormatVersion(5);
                var dbOptions = new RocksDbSharp.DbOptions()
                .SetCreateIfMissing(false)
                .SetBlockBasedTableFactory(tableOptions);

                long deleted = 0;
                using (var destDb = RocksDbSharp.RocksDb.Open(dbOptions, destPath))
                {
                    using var batch = new RocksDbSharp.WriteBatch();
                    int batchCount = 0;
                    using var iter = destDb.NewIterator();
                    for (iter.SeekToFirst(); iter.Valid(); iter.Next())
                    {
                        var key = iter.Key();
                        if (key.Length == 32 && !liveKeys.Contains(new Hash32(key)))
                        {
                            batch.Delete(key);
                            deleted++;
                            batchCount++;
                            if (batchCount >= 50_000)
                            {
                                destDb.Write(batch);
                                batch.Clear();
                                batchCount = 0;
                                if (deleted % 1_000_000 == 0)
                                {
                                    Console.Error.WriteLine($"[GC] Deleted {deleted / 1_000_000}M garbage...");
                                    Console.Error.Flush();
                                }
                            }
                        }
                    }
                    if (batchCount > 0) destDb.Write(batch);

                    Console.Error.WriteLine("[GC] Compacting...");
                    Console.Error.Flush();
                    destDb.CompactRange(new byte[0], new byte[] { 0xFF });
                }

                sw.Stop();
                long destSize = Directory.GetFiles(destPath, "*", SearchOption.AllDirectories)
                .Sum(f => new FileInfo(f).Length);

                // ── LIMPA ARQUIVOS TEMPORÁRIOS ────────────────────────────────────
                if (File.Exists(exportFile)) File.Delete(exportFile);
                if (File.Exists(liveKeysFile)) File.Delete(liveKeysFile);

                Console.Error.WriteLine($"[GC] Done: {liveKeys.Count:N0} kept, {deleted:N0} removed | {sw.Elapsed.TotalMinutes:F1}min");
                Console.Error.Flush();

                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success        = true,
                    NodesCopied    = liveKeys.Count,
                    DeletedNodes   = deleted,
                    ElapsedMinutes = sw.Elapsed.TotalMinutes,
                    DestSizeGiB    = destSize / 1024.0 / 1024.0 / 1024.0
                }));
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(JsonConvert.SerializeObject(new {
                    Success = false,
                    Error   = ex.Message
                }));
                return 1;
            }
        }

        /// <summary>
        /// Cria um checkpoint compatível de um RocksDB usando a API nativa + validação Libplanet
        /// </summary>
        static string CheckpointSingleRocksDb(string dbPath, string checkpointPath)
        {
            // Use parent directory of checkpointPath for temp dirs to ensure same filesystem
            var checkpointDirInfo = new DirectoryInfo(checkpointPath);
            var parentDir = checkpointDirInfo.Parent?.FullName ?? "/tmp";

            var tempDirName = $".checkpoint-temp-{Path.GetFileName(checkpointPath)}-{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}";
            var tempDir = Path.Combine(parentDir, tempDirName);
            var checkpointTempPath = Path.Combine(tempDir, "checkpoint");

            try
            {
                // Limpar diretório de destino se existir
                if (Directory.Exists(checkpointPath))
                    Directory.Delete(checkpointPath, true);

                // Limpar temp dir se existir
                if (Directory.Exists(tempDir))
                    Directory.Delete(tempDir, true);

                Directory.CreateDirectory(tempDir);

                // PASSO 1: Criar checkpoint usando RocksDB Checkpoint API
                IntPtr options = RocksDbNative.rocksdb_options_create();
                try
                {
                    RocksDbNative.rocksdb_options_set_skip_checking_sst_file_sizes_on_db_open(options, 1);
                    RocksDbNative.rocksdb_options_set_paranoid_checks(options, 0);

                    IntPtr errOpen = IntPtr.Zero;
                    IntPtr db = RocksDbNative.rocksdb_open_as_secondary(
                        options,
                        dbPath,
                        tempDir + "/secondary",
                        out errOpen);

                    string openError = RocksDbNative.ReadAndFreeError(ref errOpen);
                    if (openError != null)
                        throw new Exception($"Failed to open DB as secondary: {openError}");

                    try
                    {
                        // Sincronizar com o primário
                        IntPtr errSync = IntPtr.Zero;
                        RocksDbNative.rocksdb_try_catch_up_with_primary(db, out errSync);
                        string syncError = RocksDbNative.ReadAndFreeError(ref errSync);
                        if (syncError != null)
                            throw new Exception($"Failed to sync with primary: {syncError}");

                        // Criar checkpoint
                        IntPtr errCkpt = IntPtr.Zero;
                        IntPtr checkpoint = RocksDbNative.rocksdb_checkpoint_object_create(db, out errCkpt);
                        string ckptError = RocksDbNative.ReadAndFreeError(ref errCkpt);
                        if (ckptError != null)
                            throw new Exception($"Failed to create checkpoint object: {ckptError}");

                        try
                        {
                            // RocksDB checkpoint API requires that the directory DOES NOT exist
                            // It will create the directory itself
                            if (Directory.Exists(checkpointTempPath))
                                Directory.Delete(checkpointTempPath, true);

                            IntPtr errCreate = IntPtr.Zero;
                            RocksDbNative.rocksdb_checkpoint_create(
                                checkpoint,
                                checkpointTempPath,
                                0,
                                out errCreate);
                            string createError = RocksDbNative.ReadAndFreeError(ref errCreate);
                            if (createError != null)
                                throw new Exception($"Failed to create checkpoint: {createError}");
                        }
                        finally
                        {
                            RocksDbNative.rocksdb_checkpoint_object_destroy(checkpoint);
                        }
                    }
                    finally
                    {
                        RocksDbNative.rocksdb_close(db);
                    }
                }
                finally
                {
                    RocksDbNative.rocksdb_options_destroy(options);
                }

                // PASSO 2: Validar abrindo com Libplanet.RocksDBStore
                using (var rocksDb = new RocksDBStore(checkpointTempPath))
                {
                    // Se chegou aqui, o DB foi aberto com sucesso e é compatível
                    // Testar lendo algo básico
                    _ = rocksDb.GetCanonicalChainId();
                }

                // PASSO 3: Mover checkpoint validado para destino final
                if (Directory.Exists(checkpointPath))
                    Directory.Delete(checkpointPath, true);

                Directory.Move(checkpointTempPath, checkpointPath);

                // Limpar temporários
                if (Directory.Exists(tempDir))
                    Directory.Delete(tempDir, true);

                return checkpointPath;
            }
            catch (Exception)
            {
                // Cleanup em caso de erro
                if (Directory.Exists(tempDir))
                    Directory.Delete(tempDir, true);
                throw;
            }
        }

        /// <summary>
        /// Copia recursivamente um diretório.
        /// </summary>
        static void CopyDirectory(string sourceDir, string destDir)
        {
            Directory.CreateDirectory(destDir);

            foreach (var file in Directory.GetFiles(sourceDir))
            {
                string dest = Path.Combine(destDir, Path.GetFileName(file));
                File.Copy(file, dest, true);
            }

            foreach (var dir in Directory.GetDirectories(sourceDir))
            {
                string dest = Path.Combine(destDir, Path.GetFileName(dir));
                CopyDirectory(dir, dest);
            }
        }
    }

    // Helper para ByteUtil
    public static class ByteUtil
    {
        public static string Hex(byte[] bytes)
        {
            if (bytes == null) return null;
            return BitConverter.ToString(bytes).Replace("-", "").ToLowerInvariant();
        }
    }
}
