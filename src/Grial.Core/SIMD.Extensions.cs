namespace Grial.Core;

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

public static class SIMD_Extensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SimdEquals(this ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length != b.Length)
            return false;

        var length = a.Length;

        if (length == 0)
            return true;

        ref var ra = ref MemoryMarshal.GetReference(a);
        ref var rb = ref MemoryMarshal.GetReference(b);

        var offset = 0;

        if (Vector256.IsHardwareAccelerated)
        {
            while (length - offset >= 32)
            {
                var va = Vector256.LoadUnsafe(ref ra, (uint)offset);
                var vb = Vector256.LoadUnsafe(ref rb, (uint)offset);

                if (!va.Equals(vb))
                    return false;

                offset += 32;
            }
        }

        if (Vector128.IsHardwareAccelerated)
        {
            while (length - offset >= 16)
            {
                var va = Vector128.LoadUnsafe(ref ra, (uint)offset);
                var vb = Vector128.LoadUnsafe(ref rb, (uint)offset);

                if (!va.Equals(vb))
                    return false;

                offset += 16;
            }
        }

        for (; offset < length; offset++)
        {
            if (Unsafe.Add(ref ra, offset) != Unsafe.Add(ref rb, offset))
                return false;
        }

        return true;
    }
}