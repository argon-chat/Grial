namespace Grial.Test;

using System.IO.Pipelines;

public sealed class TestDuplexPipeStream
{
    public readonly Stream A_Input;
    public readonly Stream A_Output;

    public readonly Stream B_Input;
    public readonly Stream B_Output;

    readonly Pipe AB = new Pipe();
    readonly Pipe BA = new Pipe();

    public TestDuplexPipeStream()
    {
        A_Input = BA.Reader.AsStream();
        A_Output = AB.Writer.AsStream();

        B_Input = AB.Reader.AsStream();
        B_Output = BA.Writer.AsStream();
    }
}