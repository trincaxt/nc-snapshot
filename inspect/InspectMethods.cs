using System;
using System.Linq;
using System.Reflection;

class InspectMethods
{
    static void Main()
    {
        var assembly = Assembly.LoadFrom("/home/vrunnx/v4/nc-snapshot/bridge-bin/Libplanet.RocksDBStore.dll");
        var types = assembly.GetTypes();
        var rocksDbStoreType = types.FirstOrDefault(t => t.Name == "RocksDBStore");
        
        if (rocksDbStoreType != null)
        {
            var methods = rocksDbStoreType.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            foreach (var m in methods.Where(m => !m.IsSpecialName).OrderBy(m => m.Name))
            {
                var ps = string.Join(", ", m.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"));
                Console.WriteLine($"{m.Name}({ps})");
            }
        }
    }
}