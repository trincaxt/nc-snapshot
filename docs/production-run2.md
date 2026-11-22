15:31:21 ❯ env NC_GRAPH_THREADS=16 ./target/release/nc-snapshot create --prune --live --scan-gc \
                   --mode partition \
                   --block-before 0 \
                   --apv "200500/AB2da648b9154F2cCcAFBD85e0Bc3d51f97330Fc/MEUCIQCr79VTfFIcyOEk.tiy5lh9Prgm5d0lDxPDjAI9myQQMgIgPmggku8ZjVvBzV182gtDj4laNV.NDrHR5IXrD8euDU8=/ZHU5OnRpbWVzdGFtcHUxMDoyMDI2LTEwLTE5ZQ==" \
                   -s /mnt/kingston/home/vrunnx/blocoteste/ \
                   --output-dir /mnt/raid0/snapshots/ \
                   --level 3 \
                   --epoch-limit 20772
╔══════════════════════════════════════════╗
║   ⚡ NC Blockchain Snapshot Tool ⚡       ║
╚══════════════════════════════════════════╝
  Source  : /mnt/kingston/home/vrunnx/blocoteste/
  Output  : /mnt/raid0/snapshots/partition_snapshot.tar.zst
  Mode    : partition
  Level   : zstd-3
  Threads : 16
  Prune   : ON (3-scan direct GC, no export file)
  Live    : ON (node may be running, using checkpoints)
  Epoch≥  : 20772
  Validate≥: 20772

🟢 Live mode: skipping node detection (using checkpoints for consistency)
   ⚠️ Snapshot may be slightly behind the chain tip
📸 Creating live checkpoint via secondary mode 🦀...
   Using RocksDB secondary mode for consistent checkpoints
   Expected time: ~seconds
  Partition mode: linking states + all DBs + epochs >= 20772
    Linking checkpoint for states/...
    🦀 states via secondary mode 🦀...
    ✓ checkpoint created in 1.4s
    ✓ states linked
    Linking checkpoint for chain/...
    🦀 chain via secondary mode 🦀...
    ✓ checkpoint created in 0.2s
    ✓ chain linked
    Linking checkpoint for blockcommit/...
    🦀 blockcommit via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ blockcommit linked
    Linking checkpoint for txexec/...
    🦀 txexec via secondary mode 🦀...
    ✓ checkpoint created in 0.2s
    ✓ txexec linked
    Linking checkpoint for txbindex/...
    🦀 txbindex via secondary mode 🦀...
    ✓ checkpoint created in 0.7s
    ✓ txbindex linked
    Linking checkpoint for block/blockindex/...
    🦀 block/blockindex via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ block/blockindex linked
    Linking checkpoint for tx/txindex/...
    🦀 tx/txindex via secondary mode 🦀...
    ✓ checkpoint created in 0.4s
    ✓ tx/txindex linked
    Linking checkpoint for blockpercept/...
    🦀 blockpercept via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ blockpercept linked
    Linking checkpoint for nextstateroothash/...
    🦀 nextstateroothash via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ nextstateroothash linked
    Linking checkpoint for evidencec/...
    🦀 evidencec via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ evidencec linked
    Linking checkpoint for evidencep/...
    🦀 evidencep via secondary mode 🦀...
    ✓ checkpoint created in 0.0s
    ✓ evidencep linked
  📦 Processing block epochs (>= 20772)...
  📦 Processing 8 epochs (>= 20772)...
      [1/8] epoch20772
    ✓ checkpoint created in 0.0s
      [2/8] epoch20773
    ✓ checkpoint created in 0.0s
      [3/8] epoch20774
    ✓ checkpoint created in 0.0s
      [4/8] epoch20775
    ✓ checkpoint created in 0.0s
      [5/8] epoch20776
    ✓ checkpoint created in 0.0s
      [6/8] epoch20777
    ✓ checkpoint created in 0.0s
      [7/8] epoch20778
    ✓ checkpoint created in 0.1s
      [8/8] epoch20779
    ✓ checkpoint created in 0.0s
  ✅ block/ epochs linked
  📦 Processing tx epochs (>= 20772)...
  📦 Processing 19 epochs (>= 20772)...
      [1/19] epoch20772
    ✓ checkpoint created in 0.0s
      [2/19] epoch20773
    ✓ checkpoint created in 0.0s
      [3/19] epoch20774
    ✓ checkpoint created in 0.0s
      [4/19] epoch20775
    ✓ checkpoint created in 0.0s
      [5/19] epoch20776
    ✓ checkpoint created in 0.0s
      [6/19] epoch20777
    ✓ checkpoint created in 0.0s
      [7/19] epoch20778
    ✓ checkpoint created in 0.2s
      [8/19] epoch20779
    ✓ checkpoint created in 0.3s
      [9/19] epoch20780
    ✓ checkpoint created in 0.0s
      [10/19] epoch20781
    ✓ checkpoint created in 0.0s
      [11/19] epoch20782
    ✓ checkpoint created in 0.0s
      [12/19] epoch20783
    ✓ checkpoint created in 0.0s
      [13/19] epoch20784
    ✓ checkpoint created in 0.0s
      [14/19] epoch20785
    ✓ checkpoint created in 0.0s
      [15/19] epoch20786
    ✓ checkpoint created in 0.0s
      [16/19] epoch20788
    ✓ checkpoint created in 0.0s
      [17/19] epoch21173
    ✓ checkpoint created in 0.0s
      [18/19] epoch22348
    ✓ checkpoint created in 0.0s
      [19/19] epoch84401
    ✓ checkpoint created in 0.0s
  ✅ tx/ epochs linked
🟢 Fetching metadata from checkpoint (no LOCK conflicts)...
  ✅ Metadata fetched, output: /mnt/raid0/snapshots/snapshot-20779-20779.tar.zst
🧹 Running GC Pipeline on checkpoint states/...
  Getting state root from checkpoint...
  StateRoot: 459d3c1dc33b7465...
  Block #20409105
  Source: /mnt/kingston/home/vrunnx/.nc-snapshot-live-checkpoint/states
  Dest : /mnt/kingston/home/vrunnx/.nc-snapshot-live-checkpoint/states_gc
  Root : 459d3c1dc33b7465...
  ⏳ Running 3-Scan GC Pipeline directly on RocksDB (no export file)...

🧹 GC por varredura (sem arquivo de export)
   [varredura 1: chaves] 10M entradas | 1498K/s
   [varredura 1: chaves] 20M entradas | 1504K/s
   [varredura 1: chaves] 30M entradas | 1508K/s
   [varredura 1: chaves] 40M entradas | 1504K/s
   [varredura 1: chaves] 50M entradas | 1505K/s
   [varredura 1: chaves] 60M entradas | 1503K/s
   [varredura 1: chaves] 70M entradas | 1503K/s
   [varredura 1: chaves] 80M entradas | 1492K/s
   [varredura 1: chaves] 90M entradas | 1505K/s
   [varredura 1: chaves] 100M entradas | 1509K/s
   [varredura 1: chaves] 110M entradas | 1510K/s
   [varredura 1: chaves] 120M entradas | 1516K/s
   [varredura 1: chaves] 130M entradas | 1521K/s
   [varredura 1: chaves] 140M entradas | 1521K/s
   [varredura 1: chaves] 150M entradas | 1520K/s
   [varredura 1: chaves] 160M entradas | 1520K/s
   [varredura 1: chaves] 170M entradas | 1519K/s
   [varredura 1: chaves] 180M entradas | 1518K/s
   [varredura 1: chaves] 190M entradas | 1520K/s
   [varredura 1: chaves] 200M entradas | 1524K/s
   [varredura 1: chaves] 210M entradas | 1525K/s
   [varredura 1: chaves] 220M entradas | 1527K/s
   [varredura 1: chaves] 230M entradas | 1528K/s
   [varredura 1: chaves] 240M entradas | 1531K/s
   [varredura 1: chaves] 250M entradas | 1532K/s
   [varredura 1: chaves] 260M entradas | 1536K/s
   [varredura 1: chaves] 270M entradas | 1537K/s
   [varredura 1: chaves] 280M entradas | 1536K/s
   [varredura 1: chaves] 290M entradas | 1535K/s
   [varredura 1: chaves] 300M entradas | 1534K/s
   [varredura 1: chaves] 310M entradas | 1532K/s
   [varredura 1: chaves] 320M entradas | 1531K/s
   [varredura 1: chaves] 330M entradas | 1531K/s
   [varredura 1: chaves] 340M entradas | 1531K/s
   Índice: 342424550 chaves em RAM (4.18 GB) | 223.6s
   [varredura 2: grafo] 10M entradas | 1291K/s
   [varredura 2: grafo] 20M entradas | 1306K/s
   [varredura 2: grafo] 30M entradas | 1302K/s
   [varredura 2: grafo] 40M entradas | 1293K/s
   [varredura 2: grafo] 50M entradas | 1290K/s
   [varredura 2: grafo] 60M entradas | 1289K/s
   [varredura 2: grafo] 70M entradas | 1290K/s
   [varredura 2: grafo] 80M entradas | 1289K/s
   [varredura 2: grafo] 90M entradas | 1290K/s
   [varredura 2: grafo] 100M entradas | 1291K/s
   [varredura 2: grafo] 110M entradas | 1292K/s
   [varredura 2: grafo] 120M entradas | 1293K/s
   [varredura 2: grafo] 130M entradas | 1295K/s
   [varredura 2: grafo] 140M entradas | 1295K/s
   [varredura 2: grafo] 150M entradas | 1295K/s
   [varredura 2: grafo] 160M entradas | 1296K/s
   [varredura 2: grafo] 170M entradas | 1296K/s
   [varredura 2: grafo] 180M entradas | 1296K/s
   [varredura 2: grafo] 190M entradas | 1294K/s
   [varredura 2: grafo] 200M entradas | 1295K/s
   [varredura 2: grafo] 210M entradas | 1295K/s
   [varredura 2: grafo] 220M entradas | 1294K/s
   [varredura 2: grafo] 230M entradas | 1295K/s
   [varredura 2: grafo] 240M entradas | 1295K/s
   [varredura 2: grafo] 250M entradas | 1294K/s
   [varredura 2: grafo] 260M entradas | 1294K/s
   [varredura 2: grafo] 270M entradas | 1292K/s
   [varredura 2: grafo] 280M entradas | 1292K/s
   [varredura 2: grafo] 290M entradas | 1292K/s
   [varredura 2: grafo] 300M entradas | 1292K/s
   [varredura 2: grafo] 310M entradas | 1292K/s
   [varredura 2: grafo] 320M entradas | 1292K/s
   [varredura 2: grafo] 330M entradas | 1292K/s
   [varredura 2: grafo] 340M entradas | 1291K/s
   Grafo: 342424550 nós, 1120795084 arestas | 7.22 GB em RAM | 265.1s
✓ Nível 1: leu 1 nós | achou 2 novos | 3 vivos | 0.0s
✓ Nível 2: leu 2 nós | achou 16 novos | 19 vivos | 0.0s
✓ Nível 3: leu 16 nós | achou 32 novos | 51 vivos | 0.0s
✓ Nível 4: leu 32 nós | achou 128 novos | 179 vivos | 0.0s
✓ Nível 5: leu 128 nós | achou 145 novos | 324 vivos | 0.0s
✓ Nível 6: leu 145 nós | achou 176 novos | 500 vivos | 0.0s
✓ Nível 7: leu 176 nós | achou 230 novos | 730 vivos | 0.0s
✓ Nível 8: leu 230 nós | achou 857 novos | 1587 vivos | 0.0s
✓ Nível 9: leu 857 nós | achou 1729 novos | 3316 vivos | 0.0s
✓ Nível 10: leu 1729 nós | achou 10780 novos | 14096 vivos | 0.0s
✓ Nível 11: leu 10780 nós | achou 22580 novos | 36676 vivos | 0.0s
✓ Nível 12: leu 22580 nós | achou 112869 novos | 149545 vivos | 0.1s
✓ Nível 13: leu 112869 nós | achou 184110 novos | 333655 vivos | 0.4s
✓ Nível 14: leu 184110 nós | achou 614333 novos | 947988 vivos | 0.5s
✓ Nível 15: leu 614333 nós | achou 788782 novos | 1736770 vivos | 1.0s
✓ Nível 16: leu 788782 nós | achou 1762158 novos | 3498928 vivos | 0.7s
✓ Nível 17: leu 1762158 nós | achou 2125588 novos | 5624516 vivos | 0.5s
✓ Nível 18: leu 2125588 nós | achou 3370197 novos | 8994713 vivos | 0.3s
✓ Nível 19: leu 3370197 nós | achou 12829614 novos | 21824327 vivos | 0.5s
✓ Nível 20: leu 12829614 nós | achou 14259698 novos | 36084025 vivos | 0.9s
✓ Nível 21: leu 14259698 nós | achou 14489143 novos | 50573168 vivos | 0.8s
✓ Nível 22: leu 14489143 nós | achou 7442713 novos | 58015881 vivos | 0.7s
✓ Nível 23: leu 7442713 nós | achou 2009624 novos | 60025505 vivos | 0.3s
✓ Nível 24: leu 2009624 nós | achou 1755385 novos | 61780890 vivos | 0.1s
✓ Nível 25: leu 1755385 nós | achou 3188874 novos | 64969764 vivos | 0.2s
✓ Nível 26: leu 3188874 nós | achou 2384220 novos | 67353984 vivos | 0.2s
✓ Nível 27: leu 2384220 nós | achou 1027427 novos | 68381411 vivos | 0.1s
✓ Nível 28: leu 1027427 nós | achou 495250 novos | 68876661 vivos | 0.1s
✓ Nível 29: leu 495250 nós | achou 24851 novos | 68901512 vivos | 0.0s
✓ Nível 30: leu 24851 nós | achou 6706 novos | 68908218 vivos | 0.0s
✓ Nível 31: leu 6706 nós | achou 1551 novos | 68909769 vivos | 0.0s
✓ Nível 32: leu 1551 nós | achou 11 novos | 68909780 vivos | 0.0s
✓ Nível 33: leu 11 nós | achou 0 novos | 68909780 vivos | 0.0s
✅ 68909780 nós vivos de 342424550 (273514770 mortos)
   Poda: 68909780 entradas copiadas | 256.0s
🔍 Phase 4: Validating pruned states/ 🦀...
  ✅ Validation passed!  |  0.0s
✅ 3-Scan GC Pipeline complete! 🦀
  📊 Scan 1 (Keys Index): 223.6s
  📊 Scan 2 (Graph CSR):  265.1s
  📊 In-Memory BFS:       7.4s
  📊 Scan 3 (Prune):      256.8s
  📊 Phase 4 (Validate):  0.0s
  ⏱️ Total time:           12.6 min
  💾 Nodes: 342424550 → 68909780 (273514770 deleted, 79.9% removed)
✅ states/ pruned and swapped
✅ Live checkpoint created
   Checkpoint: /mnt/kingston/home/vrunnx/.nc-snapshot-live-checkpoint
   Archiving from staging (9c-blockchain untouched)
   Archiving from staging (9c-blockchain untouched)
📦 Creating partition snapshot (block + tx epochs)...
   Epoch limit: >= 20772 (epochs below this are skipped)
📂 Scanning files... 278 files | 2.13 GiB
  [00:00:03] [████████████████████████████████████████] 2.13 GiB/2.13 GiB (635.76 MiB/s) ETA 0s
✅ Snapshot criado: /mnt/raid0/snapshots/partition/snapshot-20779-20779.tar.zst
   Original   : 2.13 GiB
   Comprimido : 1.19 GiB
   Redução    : 44.2%
   Tempo      : 30.1s
   Throughput : 72 MB/s
   Manifest   : /mnt/raid0/snapshots/partition/snapshot-20779-20779.tar.blake3
📦 Creating state snapshot (indexes + states)...
📂 Scanning files... 1094 files | 72.69 GiB
  [00:08:57] [████████████████████████████████████████] 72.69 GiB/72.69 GiB (138.44 MiB/s) ETA 0s
✅ Snapshot criado: /mnt/raid0/snapshots/state/state_latest.tar.zst
   Original   : 72.69 GiB
   Comprimido : 60.24 GiB
   Redução    : 17.1%
   Tempo      : 556.5s
   Throughput : 134 MB/s
   Manifest   : /mnt/raid0/snapshots/state/state_latest.tar.blake3
📋 Metadata JSON written:
   /mnt/raid0/snapshots/metadata/snapshot-20779-20779.json
🧹 Cleaning up live checkpoint...

~/nc-snapshot main* 22m 24s
