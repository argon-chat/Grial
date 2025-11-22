namespace Grial.Test;

using Core;
using Core.KV;
using NUnit.Framework.Legacy;

[TestFixture]
public class RadixKeyIndexTests
{
    class CollectVisitor : IKeyVisitor
    {
        public readonly List<Utf8Key> Keys = new();

        public void OnKey(Utf8Key key) => Keys.Add(key);
    }

    [Test]
    public void Add_Then_PrefixScan_Works()
    {
        var r = new RadixKeyIndex();

        r.Add("apple"u8);
        r.Add("app"u8);
        r.Add("banana"u8);

        var v = new CollectVisitor();
        r.VisitByPrefix("app"u8, v);

        var keys = v.Keys.Select(x => (string)x).ToArray();

        CollectionAssert.Contains(keys, "apple");
        CollectionAssert.Contains(keys, "app");
        CollectionAssert.DoesNotContain(keys, "banana");
    }

    [Test]
    public void Prefix_On_Full_Match_And_Partial_Label()
    {
        var r = new RadixKeyIndex();
        r.Add("abcdef"u8);
        r.Add("abcxyz"u8);

        var v = new CollectVisitor();
        r.VisitByPrefix("abc"u8, v);

        var s = v.Keys.Select(x => (string)x).ToHashSet();
        Assert.That(s.Contains("abcdef"));
        Assert.That(s.Contains("abcxyz"));
    }

    [Test]
    public void Remove_Removes_Key_From_Index()
    {
        var r = new RadixKeyIndex();

        r.Add("aaa"u8);
        r.Add("aab"u8);
        r.Add("aac"u8);

        r.Remove("aab"u8);

        var v = new CollectVisitor();
        r.VisitByPrefix("aa"u8, v);

        var list = v.Keys.Select(x => (string)x).ToArray();

        CollectionAssert.Contains(list, "aaa");
        CollectionAssert.Contains(list, "aac");
        CollectionAssert.DoesNotContain(list, "aab");
    }

    [Test]
    public void Remove_And_Add_Again_Works()
    {
        var r = new RadixKeyIndex();
        r.Add("cat"u8);
        r.Remove("cat"u8);
        r.Add("cat"u8);

        var v = new CollectVisitor();
        r.VisitByPrefix("c"u8, v);

        Assert.That(v.Keys.Count, Is.EqualTo(1));
        Assert.That((string)v.Keys[0], Is.EqualTo("cat"));
    }

    [Test]
    public void Deep_Split_Nodes_Correct()
    {
        var r = new RadixKeyIndex();
        r.Add("abcd"u8);
        r.Add("abef"u8);
        r.Add("abxy"u8);

        var v = new CollectVisitor();
        r.VisitByPrefix("ab"u8, v);

        var s = v.Keys.Select(x => (string)x).ToHashSet();
        Assert.That(s.SetEquals(["abcd", "abef", "abxy"]));
    }
}