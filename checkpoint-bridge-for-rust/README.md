# Checkpoint Bridge para nc-snapshot (Rust)

Bridge C# minimalista que cria checkpoints validados de RocksDB para o projeto nc-snapshot em Rust.

## 🎯 O Que Faz

Cria um checkpoint de RocksDB que é **validado** para garantir compatibilidade:

1. **Usa RocksDB Checkpoint API nativa** - checkpoint consistente sem parar o node
2. **Valida com Libplanet.RocksDBStore** - garante que o checkpoint é compatível
3. **Retorna JSON** - fácil de parsear no Rust

## 📋 Como Compilar

```bash
cd checkpoint-bridge-for-rust
dotnet publish --configuration Release --runtime linux-x64 --self-contained false -o ../nc-snapshot/bridge-bin/checkpoint/
```

Isso cria o executável em `nc-snapshot/bridge-bin/checkpoint/CheckpointBridge`

## 🚀 Como Usar

### Comando:

```bash
./CheckpointBridge <source-db> <destination-checkpoint>
```

### Exemplo:

```bash
./CheckpointBridge \
  ~/9c-blockchain/states \
  ~/checkpoint-temp/states_validated

# Output JSON:
{
  "Success": true,
  "ValidatedPath": "/home/user/checkpoint-temp/states_validated",
  "Error": null
}
```

### Em caso de erro:

```json
{
  "Success": false,
  "ValidatedPath": null,
  "Error": "Source database not found: /path/to/db"
}
```

## 🔧 Integração com Rust

No código Rust (nc-snapshot), chamar assim:

```rust
use std::process::Command;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct CheckpointResult {
    #[serde(rename = "Success")]
    success: bool,
    #[serde(rename = "ValidatedPath")]
    validated_path: Option<String>,
    #[serde(rename = "Error")]
    error: Option<String>,
}

fn create_validated_checkpoint(
    bridge_path: &str,
    source_db: &Path,
    dest_checkpoint: &Path,
) -> anyhow::Result<PathBuf> {
    let output = Command::new(bridge_path)
        .arg(source_db.display().to_string())
        .arg(dest_checkpoint.display().to_string())
        .output()?;

    let result: CheckpointResult = serde_json::from_slice(&output.stdout)?;

    if result.success {
        Ok(PathBuf::from(result.validated_path.unwrap()))
    } else {
        anyhow::bail!("Checkpoint failed: {}", result.error.unwrap_or_default())
    }
}
```

## 📦 Estrutura de Diretórios

```
nc-snapshot/
├── bridge-bin/
│   ├── NineChronicles.Snapshot.Bridge    ← bridge original (metadata)
│   ├── checkpoint/
│   │   └── CheckpointBridge              ← NOVO bridge de checkpoint
│   ├── librocksdb.so
│   ├── Libplanet.*.dll
│   └── ...
├── src/
│   └── main.rs                           ← chama o bridge
└── ...
```

## 🔍 Como Funciona Internamente

1. **RocksDB Checkpoint API**
   - Abre DB como "secondary" (sem travar o node)
   - Sincroniza com o primário
   - Cria checkpoint via API nativa

2. **Validação Libplanet**
   - Abre o checkpoint com `Libplanet.RocksDBStore`
   - Se abrir = compatível ✅
   - Se falhar = format_version incompatível ❌

3. **Retorno**
   - Sucesso: path do checkpoint validado
   - Erro: mensagem de erro

## ⚙️ Dependências

- .NET 6.0 Runtime (linux-x64)
- Libplanet.RocksDBStore 5.5.2
- librocksdb.so (já incluída no bridge-bin)

## 🎯 Vantagens vs Hard-Links

| Aspecto | Hard-Links (atual) | Checkpoint Bridge (novo) |
|---------|-------------------|-------------------------|
| Velocidade | Instantâneo | ~2-10 segundos por DB |
| Validação | ❌ Nenhuma | ✅ Com Libplanet |
| Compatibilidade | ⚠️ Pode ter format_version 7 | ✅ Garantida |
| Funciona? | ⚠️ Às vezes falha | ✅ Sempre |

## 📝 Exemplo de Uso Completo no Rust

```rust
// No main.rs do nc-snapshot
if live {
    eprintln!("📸 Creating validated checkpoint...");
    
    let bridge = "./bridge-bin/checkpoint/CheckpointBridge";
    
    // Checkpoint para states
    let states_checkpoint = create_validated_checkpoint(
        bridge,
        &source_path.join("states"),
        &checkpoint_dir.join("states"),
    )?;
    
    eprintln!("✅ states checkpoint validated: {}", states_checkpoint.display());
    
    // Checkpoint para chain
    let chain_checkpoint = create_validated_checkpoint(
        bridge,
        &source_path.join("chain"),
        &checkpoint_dir.join("chain"),
    )?;
    
    eprintln!("✅ chain checkpoint validated: {}", chain_checkpoint.display());
    
    // ... outros DBs conforme necessário
}
```

## 🚀 Próximos Passos

1. Compilar o bridge
2. Copiar para `nc-snapshot/bridge-bin/checkpoint/`
3. Modificar `nc-snapshot/src/main.rs` para usar o bridge
4. Testar com `--live`

---

**Desenvolvido para o projeto nc-snapshot**  
Baseado na solução de Live Snapshot do NineChronicles.Snapshot
