// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

using System;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using Xunit;

namespace Microsoft.Hadoop.Avro.Tests
{
    [Trait("Category", "SpanEncodeDecode")]
    public class SpanEncodeDecodeTests
    {
        private readonly MemoryStream _stream;
        private readonly IEncoder _encoder;
        private readonly Random _random;

        private SpanDecoder _decoder => new SpanDecoder(_stream.ToArray());

        public SpanEncodeDecodeTests()
        {
            const int seed = 13;
            _stream = new MemoryStream();
            _encoder = new BinaryEncoder(_stream);
            _random = new Random(seed);
        }

        [Fact]
        public void EncodeDecode_ZeroInt()
        {
            const int expected = 0;
            _encoder.Encode(expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeInt();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void EncodeDecode_PositiveInt()
        {
            const int Expected = 105;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeInt();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_NegativeInt()
        {
            const int Expected = -106;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeInt();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_MaxInt()
        {
            const int Expected = int.MaxValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeInt();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_MinInt()
        {
            const int Expected = int.MinValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeInt();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void Decode_InvalidInt()
        {
            Assert.Throws<SerializationException>(() =>
            {
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                //causes corruption
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0x1);
                _stream.Flush();
                _stream.Seek(0, SeekOrigin.Begin);
                var result = _decoder.DecodeInt();
            }
            );
        }

        [Fact]
        public void EncodeDecode_ZeroLong()
        {
            const long Expected = 0;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeLong();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_PositiveLong()
        {
            const long Expected = 105;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeLong();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_NegativeLong()
        {
            const long Expected = -106;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeLong();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_MaxLong()
        {
            const long Expected = long.MaxValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeLong();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_MinLong()
        {
            const long Expected = long.MinValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeLong();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void Decode_InvalidLong()
        {
            Assert.Throws<SerializationException>(() =>
            {
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0xFF);
                //causes corruption
                _stream.WriteByte(0xFF);
                _stream.WriteByte(0x1);
                _stream.Flush();
                _stream.Seek(0, SeekOrigin.Begin);
                var result = _decoder.DecodeLong();
            });
        }

        [Fact]
        public void Decode_InvalidLongWithEmptyStream()
        {
            Assert.ThrowsAny<Exception>(() =>
            {
                _stream.Position = 0;
                _decoder.DecodeLong();
            });
        }

        [Fact]
        public void EncodeDecode_BooleanTrue()
        {
            const bool Expected = true;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeBool();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_BooleanFalse()
        {
            const bool Expected = false;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeBool();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_EmptyByteArray()
        {
            var expected = new byte[] { };
            _encoder.Encode(expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeByteArray();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void EncodeDecode_NotEmptyByteArray()
        {
            var expected = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            _encoder.Encode(expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeByteArray();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public void EncodeDecode_FloatMax()
        {
            const float Expected = float.MaxValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeFloat();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_FloatMin()
        {
            const float Expected = float.MinValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeFloat();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_DoubleMax()
        {
            const double Expected = double.MaxValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeDouble();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_DoubleMin()
        {
            const double Expected = double.MinValue;
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeDouble();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_EmptyString()
        {
            const string Expected = "";
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeString();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_NotEmptyString()
        {
            const string Expected = "Test string";
            _encoder.Encode(Expected);
            _encoder.Flush();

            _stream.Seek(0, SeekOrigin.Begin);
            var actual = _decoder.DecodeString();

            Assert.Equal(Expected, actual);
        }

        [Fact]
        public void EncodeDecode_HundredThousandRandomInts()
        {
            const int NumberOfTests = 100000;
            const int Seed = 13;
            var random = new Random(Seed);

            for (var i = 0; i < NumberOfTests; ++i)
            {
                _stream.SetLength(0);

                var expected = random.Next(int.MinValue, int.MaxValue);
                _encoder.Encode(expected);
                _encoder.Flush();

                _stream.Seek(0, SeekOrigin.Begin);
                var actual = _decoder.DecodeInt();

                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void EncodeDecode_HundredThousandRandomLongs()
        {
            const int NumberOfTests = 100000;
            const int Seed = 13;
            var random = new Random(Seed);

            var buffer = new byte[8];
            for (var i = 0; i < NumberOfTests; ++i)
            {
                random.NextBytes(buffer);
                var expected = BitConverter.ToInt64(buffer, 0);

                _stream.SetLength(0);
                _encoder.Encode(expected);
                _encoder.Flush();

                _stream.Seek(0, SeekOrigin.Begin);
                var actual = _decoder.DecodeLong();

                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void EncodeDecode_DifferentInts()
        {
            var values = new[]
                         {
                             new byte[] { 0, 0, 0, 1 },
                             new byte[] { 0, 0, 1, 0 },
                             new byte[] { 0, 1, 0, 0 },
                             new byte[] { 1, 0, 0, 0 }
                         };

            foreach (var value in values)
            {
                _stream.SetLength(0);

                var expected = BitConverter.ToInt32(value, 0);
                _encoder.Encode(expected);
                _encoder.Flush();

                _stream.Seek(0, SeekOrigin.Begin);

                var actual = _decoder.DecodeInt();
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void EncodeDecode_DifferentLongs()
        {
            var values = new[]
                         {
                             new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 },
                             new byte[] { 0, 0, 0, 0, 0, 0, 1, 0 },
                             new byte[] { 0, 0, 0, 0, 0, 1, 0, 0 },
                             new byte[] { 0, 0, 0, 0, 1, 0, 0, 0 },
                             new byte[] { 0, 0, 0, 1, 0, 0, 0, 0 },
                             new byte[] { 0, 0, 1, 0, 0, 0, 0, 0 },
                             new byte[] { 0, 1, 0, 0, 0, 0, 0, 0 },
                             new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 }
                         };

            foreach (var value in values)
            {
                _stream.SetLength(0);

                var expected = BitConverter.ToInt64(value, 0);
                _encoder.Encode(expected);
                _encoder.Flush();

                _stream.Seek(0, SeekOrigin.Begin);

                var actual = _decoder.DecodeLong();
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public void EncodeDecode_NotEmptyStream()
        {
            var expected = Utilities.GetRandom<byte[]>(false);

            using (var memoryStream = new MemoryStream(expected))
            {
                memoryStream.Seek(0, SeekOrigin.Begin);
                _encoder.Encode(memoryStream);
            }

            _stream.Seek(0, SeekOrigin.Begin);

            var actual = _decoder.DecodeByteArray();
            Assert.True(expected.SequenceEqual(actual));
        }

        [Fact]
        public void Skip_Double()
        {
            var valueToSkip = _random.NextDouble();
            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipDouble());
        }

        [Fact]
        public void Skip_Float()
        {
            var valueToSkip = (float)_random.NextDouble();
            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipFloat());
        }

        [Fact]
        public void Skip_Bool()
        {
            var valueToSkip = _random.Next(0, 100) % 2 == 1;
            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipBool());
        }

        [Fact]
        public void Skip_Int()
        {
            var valueToSkip = _random.Next();
            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipInt());
        }

        [Fact]
        public void Skip_Long()
        {
            long valueToSkip = _random.Next();
            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipLong());
        }

        [Fact]
        public void Skip_ByteArray()
        {
            var valueToSkip = new byte[128];
            _random.NextBytes(valueToSkip);

            CheckSkipping(
                e => e.Encode(valueToSkip),
                (ref SpanDecoder d) => d.SkipByteArray());
        }

        [Fact]
        public void Skip_String()
        {
            CheckSkipping(
                e => e.Encode("test string" + _random.Next(0, 100)),
                (ref SpanDecoder d) => d.SkipString());
        }

        [Fact]
        public void Encoder_NullByteArray()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _encoder.Encode((byte[])null);
            });
        }

        [Fact]
        public void Encode_NullStream()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _encoder.Encode((Stream)null);
            });
        }

        [Fact]
        public void Encode_NullFixed()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _encoder.EncodeFixed(null);
            });
        }

        [Fact]
        public void Decode_FixedWithNegativeSize()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                _decoder.DecodeFixed(-1);
            });
        }

        [Fact(Skip = "Add bounds check to SpanDecoder's position")]
        public void Skip_FixedWithNegativeSize()
        {
            Assert.ThrowsAny<Exception>(() =>
            {
                _decoder.SkipFixed(-1);
            });
        }

        [Fact]
        public void Encode_NullString()
        {
            Assert.Throws<ArgumentNullException>(() =>
            {
                _encoder.Encode((string)null);
            });
        }

        private void CheckSkipping(Action<IEncoder> encode, Skipper skip)
        {
            var startGuard = _random.Next();
            var endGuard = _random.Next();

            _encoder.Encode(startGuard);
            encode(_encoder);
            _encoder.Encode(endGuard);
            _encoder.Flush();
            _stream.Seek(0, SeekOrigin.Begin);

            var decoder = _decoder;
            Assert.Equal(startGuard, decoder.DecodeInt());
            skip(ref decoder);
            Assert.Equal(endGuard, decoder.DecodeInt());
        }

        private delegate void Skipper(ref SpanDecoder decoder);
    }
}
