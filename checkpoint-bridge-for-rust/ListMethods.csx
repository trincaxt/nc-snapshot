#r "Libplanet.RocksDBStore.dll"
#r "Libplanet.Store.dll"
#r "Libplanet.Common.dll"
#r "Libplanet.Types.dll"
#r "RocksDbSharp.dll"

using Libplanet.Store;
var methods = typeof(RocksDBStore).GetMethods();
foreach (var m in methods.Where(m => !m.IsSpecialName).OrderBy(m => m.Name)) {
    var ps = string.Join(", ", m.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"));
    Console.WriteLine($"{m.Name}({ps})");
}