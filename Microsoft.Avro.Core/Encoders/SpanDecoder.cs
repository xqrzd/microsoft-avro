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
using System.Runtime.Serialization;
using System.Text;

namespace Microsoft.Hadoop.Avro
{
    public ref struct SpanDecoder
    {
        private readonly ReadOnlySpan<byte> _buffer;
        private int _position;

        public SpanDecoder(ReadOnlySpan<byte> buffer)
        {
            _buffer = buffer;
            _position = 0;
        }

        public bool DecodeBool() => ReadByte() != 0;

        public int DecodeInt()
        {
            var currentByte = (uint)ReadByte();
            byte read = 1;
            uint result = currentByte & 0x7FU;
            int shift = 7;
            while ((currentByte & 0x80) != 0)
            {
                currentByte = ReadByte();
                read++;
                result |= (currentByte & 0x7FU) << shift;
                shift += 7;
                if (read > 5)
                {
                    throw new SerializationException("Invalid integer value in the input stream.");
                }
            }
            return (int)((-(result & 1)) ^ ((result >> 1) & 0x7FFFFFFFU));
        }

        public long DecodeLong()
        {
            var value = (uint)ReadByte();
            byte read = 1;
            ulong result = value & 0x7FUL;
            int shift = 7;
            while ((value & 0x80) != 0)
            {
                value = (uint)ReadByte();
                read++;
                result |= (value & 0x7FUL) << shift;
                shift += 7;
                if (read > 10)
                {
                    throw new SerializationException("Invalid integer long in the input stream.");
                }
            }
            var tmp = unchecked((long)result);
            return (-(tmp & 0x1L)) ^ ((tmp >> 1) & 0x7FFFFFFFFFFFFFFFL);
        }

        public float DecodeFloat()
        {
#if NETCOREAPP
            var value = DecodeSpan(4);
            if (!BitConverter.IsLittleEndian)
            {
                Span<byte> reversed = stackalloc byte[4];
                value.CopyTo(reversed);
                reversed.Reverse();
                return BitConverter.ToSingle(reversed);
            }
            return BitConverter.ToSingle(value);
#else
            var value = DecodeFixed(4);
            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(value);
            }
            return BitConverter.ToSingle(value, 0);
#endif
        }

        public double DecodeDouble()
        {
#if NETCOREAPP
            var value = DecodeSpan(8);
            long longValue = value[0]
                | (long)value[1] << 0x8
                | (long)value[2] << 0x10
                | (long)value[3] << 0x18
                | (long)value[4] << 0x20
                | (long)value[5] << 0x28
                | (long)value[6] << 0x30
                | (long)value[7] << 0x38;
            return BitConverter.Int64BitsToDouble(longValue);
#else
            var value = DecodeFixed(8);
            long longValue = value[0]
                | (long)value[1] << 0x8
                | (long)value[2] << 0x10
                | (long)value[3] << 0x18
                | (long)value[4] << 0x20
                | (long)value[5] << 0x28
                | (long)value[6] << 0x30
                | (long)value[7] << 0x38;
            return BitConverter.Int64BitsToDouble(longValue);
#endif
        }

        public byte[] DecodeByteArray()
        {
            int arraySize = DecodeInt();
            var array = DecodeSpan(arraySize);
            return array.ToArray();
        }

        public string DecodeString()
        {
#if NETCOREAPP
            int arraySize = DecodeInt();
            var array = DecodeSpan(arraySize);
            return Encoding.UTF8.GetString(array);
#else
            return Encoding.UTF8.GetString(DecodeByteArray());
#endif
        }

        public int DecodeArrayChunk()
        {
            return DecodeChunk();
        }

        public int DecodeMapChunk()
        {
            return DecodeChunk();
        }

        public byte[] DecodeFixed(int size)
        {
            if (size <= 0)
            {
                throw new ArgumentOutOfRangeException("size");
            }

            var span = DecodeSpan(size);
            return span.ToArray();
        }

        private int DecodeChunk()
        {
            int result = DecodeInt();
            if (result < 0)
            {
                DecodeLong();
                result = -result;
            }
            return result;
        }

        public ReadOnlySpan<byte> DecodeSpan(int length)
        {
            var slice = _buffer.Slice(_position, length);
            _position += length;
            return slice;
        }

        public void SkipBool() => _position++;

        public void SkipFloat() => _position += 4;

        public void SkipDouble() => _position += 8;

        public void SkipInt()
        {
            var currentByte = (uint)ReadByte();
            while ((currentByte & 0x80) != 0)
            {
                currentByte = (uint)ReadByte();
            }
        }

        public void SkipLong() => SkipInt();

        public void SkipByteArray()
        {
            int arraySize = DecodeInt();
            _position += arraySize;
        }

        public void SkipString() => SkipByteArray();

        public void SkipFixed(int size) => _position += size;

        private byte ReadByte() => _buffer[_position++];
    }
}
