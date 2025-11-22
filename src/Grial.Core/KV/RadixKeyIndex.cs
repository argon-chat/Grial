namespace Grial.Core.KV;

using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

public interface IKeyVisitor
{
    void OnKey(Utf8Key key);
}

public readonly struct LabelRef(int offset, int length)
{
    public readonly int Offset = offset;
    public readonly int Length = length;
}

public readonly struct KeyRef(int offset, int length, int hash)
{
    public readonly int Offset = offset;
    public readonly int Length = length;
    public readonly int Hash = hash;
}

public sealed class RadixKeyIndex
{
    private sealed class ByteArena
    {
        private byte[] buffer;
        private int position;

        public ByteArena(int initialSize = 1024 * 1024)
        {
            if (initialSize <= 0)
                initialSize = 1024 * 1024;

            buffer = GC.AllocateUninitializedArray<byte>(initialSize);
            position = 0;
        }

        public KeyRef AddKey(Utf8Key data)
        {
            var offset = Allocate(data.Length);
            data.Span.CopyTo(buffer.AsSpan(offset, data.Length));
            var hash = ComputeHash(data.Span);
            return new KeyRef(offset, data.Length, hash);
        }

        public LabelRef SliceLabel(int offset, int length)
            => new(offset, length);

        public ReadOnlySpan<byte> GetSpan(LabelRef r)
            => buffer.AsSpan(r.Offset, r.Length);

        public Utf8Key GetSpan(KeyRef r)
            => new(buffer.AsMemory(r.Offset, r.Length));

        public byte GetByte(int offset)
            => buffer[offset];

        private int Allocate(int len)
        {
            var newPos = position + len;
            if (newPos > buffer.Length)
                Grow(Math.Max(len, buffer.Length));

            var off = position;
            position = newPos;
            return off;
        }

        private void Grow(int additional)
        {
            var newBuf = GC.AllocateUninitializedArray<byte>(buffer.Length + additional);
            buffer.AsSpan().CopyTo(newBuf);
            buffer = newBuf;
        }

        // FNV-1a 32-bit
        public static int ComputeHash(Utf8Key data)
        {
            const uint fnvOffset = 2166136261;
            const uint fnvPrime = 16777619;

            var hash = fnvOffset;
            for (var i = 0; i < data.Length; i++)
            {
                hash ^= data.Span[i];
                hash *= fnvPrime;
            }

            return (int)hash;
        }
    }

    // ============== Bloom-фильтр на ноде (64 бита) ==============

    private struct BloomFilter64
    {
        private ulong bits;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddHash(int hash)
        {
            var h = unchecked((uint)hash);
            var b1 = (int)(h & 63);
            var b2 = (int)((h >> 6) & 63);
            var mask = (1UL << b1) | (1UL << b2);
            bits |= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MightContainHash(int hash)
        {
            if (bits == 0)
                return false;

            var h = unchecked((uint)hash);
            var b1 = (int)(h & 63);
            var b2 = (int)((h >> 6) & 63);
            var mask = (1UL << b1) | (1UL << b2);
            return (bits & mask) == mask;
        }
    }

    // ===================== Node ===============================
    private readonly struct ChildEdge(byte label, Node node)
    {
        public readonly byte Label = label;
        public readonly Node Node = node;
    }

    private sealed class Node(LabelRef label)
    {
        public LabelRef Label = label;
        public ChildEdge[] Children = Array.Empty<ChildEdge>(); // отсортирован по Label
        public int ChildCount = 0;
        public KeyRef[] Keys = Array.Empty<KeyRef>();
        public int KeyCount = 0;

        public bool HasChildren => ChildCount > 0;
    }


    // ===================== Main fields ==========================

    private readonly ByteArena arena = new();
    private readonly Node root = new(new LabelRef(0, 0));
    private readonly Lock gate = new();

    // ===================== Lock helper ==========================

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnterLock()
    {
        if (!gate.TryEnter())
        {
            var sw = new SpinWait();
            while (!gate.TryEnter())
                sw.SpinOnce();
        }
    }

    // ===================== Public API ============================

    public void Add(Utf8Key key)
    {
        if (key.Length == 0)
            return;

        EnterLock();
        try
        {
            var keyRef = arena.AddKey(key);
            InsertInternal(root, in keyRef, keyPos: 0);
        }
        finally
        {
            gate.Exit();
        }
    }

    public void Remove(Utf8Key key)
    {
        if (key.Length == 0)
            return;

        EnterLock();
        try
        {
            RemoveInternal(parent: null, current: root, key, keyOffset: 0);
        }
        finally
        {
            gate.Exit();
        }
    }

    public void VisitByPrefix(Utf8Key prefix, IKeyVisitor visitor)
    {
        EnterLock();
        try
        {
            if (prefix.Length == 0)
            {
                VisitSubtree(root, visitor);
                return;
            }

            var node = FindSubtree(root, prefix, 0);
            if (node != null)
                VisitSubtree(node, visitor);
        }
        finally
        {
            gate.Exit();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int FindChildIndex(Node node, byte label)
    {
        var lo = 0;
        var hi = node.ChildCount - 1;

        var children = node.Children;

        while (lo <= hi)
        {
            var mid = (lo + hi) >> 1;
            var midLabel = children[mid].Label;

            if (midLabel == label)
                return mid;
            if (midLabel < label)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return ~lo; // если не найден — битовая инверсия позиции вставки
    }

    private static Node? GetChild(Node node, byte label, out int index)
    {
        index = FindChildIndex(node, label);
        if (index < 0)
        {
            return null;
        }

        return node.Children[index].Node;
    }

    private static void InsertChild(Node node, byte label, Node child)
    {
        var idx = FindChildIndex(node, label);

        if (idx >= 0)
        {
            node.Children[idx] = new ChildEdge(label, child);
            return;
        }

        var insertPos = ~idx;
        var arr = node.Children;
        var count = node.ChildCount;

        if (arr.Length == 0)
        {
            node.Children = new[] { new ChildEdge(label, child) };
            node.ChildCount = 1;
            return;
        }

        if (count == arr.Length)
        {
            var newArr = new ChildEdge[arr.Length * 2];
            Array.Copy(arr, 0, newArr, 0, insertPos);
            newArr[insertPos] = new ChildEdge(label, child);
            Array.Copy(arr, insertPos, newArr, insertPos + 1, count - insertPos);
            node.Children = newArr;
        }
        else
        {
            Array.Copy(arr, insertPos, arr, insertPos + 1, count - insertPos);
            arr[insertPos] = new ChildEdge(label, child);
        }

        node.ChildCount++;
    }

    private static void RemoveChildAt(Node node, int index)
    {
        var arr = node.Children;
        var count = node.ChildCount;

        if (index < 0 || index >= count)
            return;

        var move = count - index - 1;
        if (move > 0)
            Array.Copy(arr, index + 1, arr, index, move);

        node.ChildCount--;
    }

    private void InsertInternal(Node current, in KeyRef keyRef, int keyPos)
    {
        while (true)
        {
            var remainingLen = keyRef.Length - keyPos;
            if (remainingLen == 0)
            {
                var keySpan = arena.GetSpan(keyRef);
                if (!NodeContainsKey(current, keySpan))
                    AddKeyToNode(current, in keyRef);
                return;
            }

            var remainingSpan = arena.GetSpan(new KeyRef(keyRef.Offset + keyPos, remainingLen, keyRef.Hash));
            var first = remainingSpan.Span[0];

            var child = GetChild(current, first, out var childIndex);
            if (child == null)
            {
                var label = new LabelRef(keyRef.Offset + keyPos, remainingLen);
                var leaf = new Node(label);
                AddKeyToNode(leaf, in keyRef);
                InsertChild(current, first, leaf);
                return;
            }

            var labelSpan = arena.GetSpan(child.Label);
            var common = LongestCommonPrefix(labelSpan, remainingSpan.Span);

            if (common == labelSpan.Length)
            {
                keyPos += common;
                current = child;
                continue;
            }

            SplitAndInsert(current, child, first, common, in keyRef, keyPos);
            return;
        }
    }

    private void SplitAndInsert(Node parent, Node child, byte childLabel, int common, in KeyRef keyRef, int keyPos)
    {
        var oldLabel = child.Label;

        var prefix = new LabelRef(oldLabel.Offset, common);
        var suffix = new LabelRef(oldLabel.Offset + common, oldLabel.Length - common);

        var suffixNode = new Node(suffix)
        {
            Children = child.Children,
            ChildCount = child.ChildCount,
            Keys = child.Keys,
            KeyCount = child.KeyCount
        };

        child.Label = prefix;
        child.Children = Array.Empty<ChildEdge>();
        child.ChildCount = 0;
        child.Keys = Array.Empty<KeyRef>();
        child.KeyCount = 0;

        var suffixFirst = arena.GetByte(suffix.Offset);
        InsertChild(child, suffixFirst, suffixNode);

        var remAfterCommon = keyRef.Length - (keyPos + common);
        if (remAfterCommon == 0)
        {
            var keySpan = arena.GetSpan(keyRef);
            if (!NodeContainsKey(child, keySpan))
                AddKeyToNode(child, in keyRef);
            return;
        }

        var tailOff = keyRef.Offset + keyPos + common;
        var tailRef = new LabelRef(tailOff, remAfterCommon);
        var leaf = new Node(tailRef);
        AddKeyToNode(leaf, in keyRef);

        var tailFirst = arena.GetByte(tailRef.Offset);
        InsertChild(child, tailFirst, leaf);
    }

    private static int LongestCommonPrefix(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        var len = Math.Min(a.Length, b.Length);
        var i = 0;

        if (Vector256.IsHardwareAccelerated)
        {
            while (len - i >= 32)
            {
                var va = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(a), (uint)i);
                var vb = Vector256.LoadUnsafe(ref MemoryMarshal.GetReference(b), (uint)i);

                if (!va.Equals(vb))
                    goto Tail;

                i += 32;
            }
        }

        if (Vector128.IsHardwareAccelerated)
        {
            while (len - i >= 16)
            {
                var va = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(a), (uint)i);
                var vb = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(b), (uint)i);

                if (!va.Equals(vb))
                    goto Tail;

                i += 16;
            }
        }

        Tail:
        for (; i < len; i++)
        {
            if (a[i] != b[i])
                return i;
        }

        return len;
    }

    private bool NodeContainsKey(Node node, Utf8Key key)
    {
        var keyLen = key.Length;
        var keyHash = ByteArena.ComputeHash(key);

        for (var i = 0; i < node.KeyCount; i++)
        {
            var kr = node.Keys[i];
            if (kr.Length != keyLen || kr.Hash != keyHash)
                continue;

            var span = arena.GetSpan(kr);
            if (SimdEquals(span.Span, key.Span))
                return true;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool SimdEquals(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
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

    private static void AddKeyToNode(Node node, in KeyRef keyRef)
    {
        if (node.KeyCount == node.Keys.Length)
        {
            var newSize = node.Keys.Length == 0 ? 4 : node.Keys.Length * 2;
            var newArr = new KeyRef[newSize];
            if (node.KeyCount > 0)
                Array.Copy(node.Keys, newArr, node.KeyCount);
            node.Keys = newArr;
        }

        node.Keys[node.KeyCount++] = keyRef;
    }

    private bool RemoveInternal(Node? parent, Node current, Utf8Key key, int keyOffset)
    {
        var remaining = key.Span[keyOffset..];
        if (remaining.Length == 0)
            return false;

        var first = remaining[0];
        var childIndex = FindChildIndex(current, first);
        if (childIndex < 0)
            return false;

        var child = current.Children[childIndex].Node;
        var labelSpan = arena.GetSpan(child.Label);
        if (!remaining.StartsWith(labelSpan))
            return false;

        var newOffset = keyOffset + labelSpan.Length;
        var newRem = key.Length - newOffset;

        if (newRem == 0)
        {
            child.KeyCount = 0;
            child.Keys = [];

            if (!child.HasChildren && parent != null)
                RemoveChildAt(parent, childIndex);

            return true;
        }

        if (!RemoveInternal(child, child, key, newOffset))
            return false;

        if (child is { HasChildren: false, KeyCount: 0 })
            RemoveChildAt(current, childIndex);

        return true;
    }

    private void VisitSubtree(Node node, IKeyVisitor visitor)
    {
        for (var i = 0; i < node.KeyCount; i++)
        {
            var kr = node.Keys[i];
            visitor.OnKey(arena.GetSpan(kr));
        }

        for (var i = 0; i < node.ChildCount; i++)
        {
            var child = node.Children[i].Node;
            VisitSubtree(child, visitor);
        }
    }

    private Node? FindSubtree(Node current, Utf8Key prefix, int keyOffset)
    {
        var rem = prefix.Span[keyOffset..];
        if (rem.Length == 0)
            return current;

        var first = rem[0];
        var idx = FindChildIndex(current, first);
        if (idx < 0)
            return null;

        var child = current.Children[idx].Node;
        var label = arena.GetSpan(child.Label);
        var common = LongestCommonPrefix(label, rem);

        if (common == 0)
            return null;

        if (common == rem.Length)
            return child;

        if (common < label.Length)
            return null;

        return FindSubtree(child, prefix, keyOffset + label.Length);
    }
}